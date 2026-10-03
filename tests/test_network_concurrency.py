"""Real-network concurrency semantics of the gateway entry.

Unlike ``test_network_http.py`` (TestClient, sequential requests), these
tests put the gateway behind a *real* uvicorn server on a *real* TCP port
and fire requests from multiple client threads at once. They pin down:

* concurrent slow requests really overlap: N requests against a slow
  upstream finish in ~1x (not N x) the single-request latency;
* one slow/stuck upstream never freezes the ingress: unrelated routes and
  the operational endpoints (/healthz, /__admin/state) stay responsive;
* the three admission categories (rate / inflight / quota) are enforced at
  the real network entry, reject without touching the upstream, and never
  over-admit while the upstream is slow.
"""

from __future__ import annotations

import socket
import subprocess
import sys
import threading
import time

import httpx
import pytest
import uvicorn

from edge_gateway.app import create_network_app
from edge_gateway.realupstream import HttpUpstreamRegistry
from edge_gateway.upstream import UpstreamError
from tests.conftest import log_case

CLUSTERS = {
    "users-v1": ["users-v1-1", "users-v1-2"],
    "orders-v1": ["orders-v1-1"],
}
ALL_INSTANCES = [iid for ids in CLUSTERS.values() for iid in ids]


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


class LiveGateway:
    """Gateway served by a real uvicorn process-thread + real upstreams."""

    def __init__(self) -> None:
        self.ports = {iid: _free_port() for iid in ALL_INSTANCES}
        self.base_urls = {
            iid: f"http://127.0.0.1:{self.ports[iid]}" for iid in ALL_INSTANCES
        }
        self.procs: dict[str, subprocess.Popen] = {}
        self.gateway_port = _free_port()
        self.registry = HttpUpstreamRegistry()
        for cluster, ids in CLUSTERS.items():
            for iid in ids:
                self.registry.register(iid, self.base_urls[iid], cluster)
        self._server: uvicorn.Server | None = None
        self._thread: threading.Thread | None = None
        self.client: httpx.Client | None = None

    def start(self) -> None:
        for iid in ALL_INSTANCES:
            self.procs[iid] = subprocess.Popen(
                [sys.executable, "-m", "edge_gateway.realupstream",
                 "--id", iid, "--port", str(self.ports[iid])],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                for iid in ALL_INSTANCES:
                    self.registry.probe(iid)
                break
            except UpstreamError:
                time.sleep(0.05)
        else:
            raise AssertionError("upstream processes never came up")

        app = create_network_app(
            base_urls=self.base_urls, start_recovery=False, clusters=CLUSTERS
        )
        self._server = uvicorn.Server(
            uvicorn.Config(
                app,
                host="127.0.0.1",
                port=self.gateway_port,
                log_level="warning",
                access_log=False,
            )
        )
        self._thread = threading.Thread(
            target=self._server.run, name="uvicorn-under-test", daemon=True
        )
        self._thread.start()
        self.client = httpx.Client(
            base_url=f"http://127.0.0.1:{self.gateway_port}", timeout=15.0
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if self._server.started:
                try:
                    if self.client.get("/healthz").status_code == 200:
                        return
                except httpx.TransportError:
                    pass
            time.sleep(0.05)
        raise AssertionError("gateway server never started")

    def stop(self) -> None:
        if self.client is not None:
            self.client.close()
        if self._server is not None:
            self._server.should_exit = True
        if self._thread is not None:
            self._thread.join(timeout=5)
        for proc in self.procs.values():
            proc.terminate()
        for proc in self.procs.values():
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()

    # -- helpers ------------------------------------------------------------

    def upstream_calls(self, instance_id: str) -> int:
        return self.registry.remote_state(instance_id)["calls"]

    def set_mode(self, instance_id: str, mode: str, delay: float = 1.0) -> None:
        self.registry.set_remote_mode(instance_id, mode, delay=delay)

    def fire_concurrently(self, n: int, method: str, path: str,
                          headers: dict) -> tuple[list[dict], float]:
        """Fire ``n`` requests released by one barrier; return results+wall."""

        barrier = threading.Barrier(n)
        results: list[dict] = [None] * n  # type: ignore[list-item]

        def worker(i: int) -> None:
            barrier.wait(timeout=10)
            t0 = time.monotonic()
            resp = self.client.request(method, path, headers=headers)
            results[i] = {
                "status": resp.status_code,
                "elapsed": time.monotonic() - t0,
                "body": resp.json() if resp.headers.get("content-type", "").startswith("application/json") else None,
                "served_by": resp.headers.get("x-served-by"),
            }

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        wall0 = time.monotonic()
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=20)
        wall = time.monotonic() - wall0
        assert all(r is not None for r in results), "a client thread hung"
        return results, wall


@pytest.fixture()
def live():
    gw = LiveGateway()
    try:
        gw.start()
    except BaseException:
        gw.stop()
        raise
    yield gw
    gw.stop()


def test_concurrent_slow_requests_run_in_parallel(live):
    """Two 0.8s-upstream requests fired together finish in ~0.8s, not 1.6s."""

    delay = 0.8
    live.set_mode("users-v1-1", "slow", delay=delay)
    try:
        results, wall = live.fire_concurrently(
            2, "GET", "/users/42", {"x-caller-id": "par-alice"}
        )
    finally:
        live.set_mode("users-v1-1", "normal")
    sequential_estimate = delay * 2
    log_case(
        "concurrent slow requests overlap (real network entry)",
        upstream_delay=delay,
        per_request_elapsed=[round(r["elapsed"], 3) for r in results],
        total_wall=round(wall, 3),
        sequential_would_take=sequential_estimate,
        verdict="PARALLEL" if wall < sequential_estimate * 0.75 else "SERIALIZED",
    )
    assert [r["status"] for r in results] == [200, 200]
    assert all(r["elapsed"] >= delay * 0.8 for r in results)
    # Clearly faster than one-after-another; allows scheduling slack.
    assert wall < sequential_estimate * 0.75


def test_slow_upstream_does_not_block_unrelated_traffic_or_ops(live):
    """While one upstream hangs, other routes and /healthz stay fast."""

    live.set_mode("users-v1-1", "slow", delay=1.5)
    bg: dict = {}

    def slow_request() -> None:
        t0 = time.monotonic()
        resp = live.client.get("/users/1", headers={"x-caller-id": "bg-slow"})
        bg["status"] = resp.status_code
        bg["elapsed"] = time.monotonic() - t0

    thread = threading.Thread(target=slow_request)
    try:
        thread.start()
        time.sleep(0.3)  # ensure the slow request is in flight upstream

        t0 = time.monotonic()
        health = live.client.get("/healthz")
        health_elapsed = time.monotonic() - t0

        t0 = time.monotonic()
        state = live.client.get("/__admin/state")
        state_elapsed = time.monotonic() - t0

        t0 = time.monotonic()
        other = live.client.get("/orders/1", headers={"x-caller-id": "bg-other"})
        other_elapsed = time.monotonic() - t0
    finally:
        thread.join(timeout=15)
        live.set_mode("users-v1-1", "normal")

    log_case(
        "slow upstream must not freeze ingress or ops endpoints",
        slow_request_status=bg["status"],
        slow_request_elapsed=round(bg["elapsed"], 3),
        healthz=f"{health.status_code} in {health_elapsed:.3f}s",
        admin_state=f"{state.status_code} in {state_elapsed:.3f}s",
        unrelated_route=f"{other.status_code} in {other_elapsed:.3f}s",
    )
    assert bg["status"] == 200 and bg["elapsed"] >= 1.2
    assert health.status_code == 200 and health_elapsed < 0.5
    assert state.status_code == 200 and state_elapsed < 0.5
    assert other.status_code == 200 and other_elapsed < 0.8


def test_inflight_limit_enforced_under_real_concurrency(live):
    """payments-api allows 2 in flight; the 3rd concurrent one is rejected
    before touching the upstream, even while the upstream is slow."""

    live.set_mode("users-v1-1", "slow", delay=1.0)
    calls_before = live.upstream_calls("users-v1-1")
    try:
        results, wall = live.fire_concurrently(
            3, "GET", "/payments", {"x-caller-id": "inflight-bob"}
        )
    finally:
        live.set_mode("users-v1-1", "normal")
    calls_after = live.upstream_calls("users-v1-1")

    accepted = [r for r in results if r["status"] == 200]
    rejected = [r for r in results if r["status"] == 429]
    log_case(
        "inflight limit at real network entry (upstream slow)",
        inflight_limit=2,
        fired=3,
        accepted=len(accepted),
        rejected=[r["body"]["error"]["code"] for r in rejected],
        upstream_calls_consumed=calls_after - calls_before,
    )
    assert len(accepted) == 2
    assert len(rejected) == 1
    assert rejected[0]["body"]["error"]["code"] == "INFLIGHT_LIMIT_EXCEEDED"
    # The rejected request never reached the upstream.
    assert calls_after - calls_before == 2


def test_rate_limit_enforced_at_real_network_entry(live):
    """4 rapid requests against a rate_limit=3/1s rule: the 4th is rejected
    and consumes no upstream resource."""

    calls_before = live.upstream_calls("users-v1-1")
    responses = [
        live.client.get("/payments", headers={"x-caller-id": "rate-carol"})
        for _ in range(4)
    ]
    calls_after = live.upstream_calls("users-v1-1")

    statuses = [r.status_code for r in responses]
    log_case(
        "short-burst rate limit at real network entry",
        rate_limit="3 per 1s",
        fired=4,
        statuses=statuses,
        last_error=responses[-1].json()["error"]["code"],
        upstream_calls_consumed=calls_after - calls_before,
    )
    assert statuses[:3] == [200, 200, 200]
    assert statuses[3] == 429
    assert responses[-1].json()["error"]["code"] == "RATE_LIMITED"
    assert calls_after - calls_before == 3


def test_quota_limit_enforced_at_real_network_entry(live):
    """quota_limit=5 per 3s window: pacing under the rate limit still hits
    the window budget exactly at the 6th request."""

    caller = {"x-caller-id": "quota-dave"}
    calls_before = live.upstream_calls("users-v1-1")
    first_burst = [live.client.get("/payments", headers=caller) for _ in range(3)]
    time.sleep(1.1)  # let the 1s rate window roll; quota window (3s) persists
    second_burst = [live.client.get("/payments", headers=caller) for _ in range(3)]
    calls_after = live.upstream_calls("users-v1-1")

    statuses = [r.status_code for r in first_burst + second_burst]
    log_case(
        "per-window quota at real network entry",
        quota_limit="5 per 3s",
        fired="3 + 3 (paced to stay under the rate limit)",
        statuses=statuses,
        last_error=second_burst[-1].json()["error"]["code"],
        upstream_calls_consumed=calls_after - calls_before,
    )
    assert statuses == [200, 200, 200, 200, 200, 429]
    assert second_burst[-1].json()["error"]["code"] == "QUOTA_EXCEEDED"
    assert calls_after - calls_before == 5
