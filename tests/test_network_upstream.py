"""Block 1: real independently startable/stopable network upstreams.

The upstreams here are genuine HTTP servers on loopback (exactly the same
``upstream_server`` module that runs standalone), so connection refused on a
stopped instance and slow/error responses behave like real network faults.
Servers run in threads for test stability; the orchestrator starts the very
same module as separate OS processes for manual drills.
"""

from __future__ import annotations

import socket
import threading
import time

import httpx
import pytest
from fastapi.testclient import TestClient

from conftest import log_case
from edge_gateway.admission import AdmissionManager
from edge_gateway.gateway import Gateway
from edge_gateway.health import HealthMonitor
from edge_gateway.http_upstream import HttpUpstreamRegistry
from edge_gateway.lab import lab_config
from edge_gateway.lab.netapp import create_network_app
from edge_gateway.lab.upstream_server import create_server
from edge_gateway.router import Router, RuleRegistry


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class RealUpstreams:
    """Manages one real HTTP server thread per instance; stoppable."""

    def __init__(self, instances: list[str]):
        self.ports = {i: _free_port() for i in instances}
        self.servers: dict = {}
        self.threads: dict = {}
        for instance_id in instances:
            self._start(instance_id)

    def _start(self, instance_id: str):
        server = create_server(self.ports[instance_id], instance_id)
        thread = threading.Thread(
            target=server.serve_forever, kwargs={"poll_interval": 0.02}, daemon=True
        )
        thread.start()
        self.servers[instance_id] = server
        self.threads[instance_id] = thread

    def stop(self, instance_id: str):
        server = self.servers.pop(instance_id, None)
        thread = self.threads.pop(instance_id, None)
        if server is not None:
            server.shutdown()
            server.server_close()
        if thread is not None:
            thread.join(timeout=2)

    def start(self, instance_id: str):
        # Re-bind the same port: a real restart that refuses until it listens.
        self._start(instance_id)

    def url(self, instance_id: str) -> str:
        return f"http://127.0.0.1:{self.ports[instance_id]}"

    def control(self, instance_id: str, payload: dict):
        resp = httpx.post(
            self.url(instance_id) + "/__control/mode", json=payload, timeout=2.0
        )
        resp.raise_for_status()
        return resp.json()

    def meta(self, instance_id: str) -> dict:
        return httpx.get(self.url(instance_id) + "/__meta", timeout=2.0).json()

    def shutdown_all(self):
        for instance_id in list(self.servers):
            self.stop(instance_id)


@pytest.fixture()
def network():
    instances = ["users-v1-1", "users-v1-2"]
    ups = RealUpstreams(instances)

    clusters = {"users-v1": instances}
    rules = [
        lab_config.default_rules()[2],  # users-read: GET /users
        lab_config.default_rules()[1],  # users-write: POST /users
    ]
    registry = HttpUpstreamRegistry(
        {i: ups.url(i) for i in instances}, timeout=0.6
    )
    health = HealthMonitor(
        instances, registry, failure_threshold=3, recovery_threshold=2,
        probe_interval=0.05,
    )
    gateway = Gateway(
        router=Router(RuleRegistry(rules)),
        quotas=AdmissionManager(),
        health=health,
        registry=registry,
        clusters=clusters,
    )
    app = create_network_app(gateway, prober_interval=0.02, start_prober=False)
    client = TestClient(app, raise_server_exceptions=False)
    try:
        yield type("Env", (), {
            "client": client, "ups": ups, "registry": registry,
            "health": health, "gateway": gateway,
        })()
    finally:
        client.close()
        registry.close()
        ups.shutdown_all()


def test_transparent_method_path_query_body_headers_passthrough(network):
    body = '{"name":"simon","n":7}'
    resp = network.client.post(
        "/users/42?team=core&ref=abc",
        content=body,
        headers={
            "x-caller-id": "carol",
            "content-type": "application/json",
            "x-custom": "keep-me",
            "x-canary": "false",
        },
    )
    assert resp.status_code == 200
    received = resp.json()
    log_case(
        "real network upstream receives method/path/query/body/headers",
        received=received,
        gateway_headers={"instance": resp.headers.get("x-gateway-instance")},
    )
    assert received["method"] == "POST"
    assert received["path"].startswith("/users/42?")
    assert "team=core" in received["path"] and "ref=abc" in received["path"]
    assert received["body"] == body
    assert received["headers"].get("x-custom") == "keep-me"
    assert received["headers"].get("content-type") == "application/json"
    assert resp.headers.get("x-upstream-instance") is not None
    assert resp.headers.get("x-gateway-instance") == "users-v1-1"


def test_stopped_process_is_connection_refused_and_fails_over(network):
    network.ups.stop("users-v1-1")
    # Wait until the OS actually refuses connections (port released).
    deadline = time.time() + 2.0
    while time.time() < deadline:
        try:
            httpx.get(network.ups.url("users-v1-1") + "/__health", timeout=0.2)
        except httpx.ConnectError:
            break
        time.sleep(0.02)

    resp = network.client.get("/users/1", headers={"x-caller-id": "bob"})
    received = resp.json()
    log_case(
        "stopped process -> connect refused -> safe failover to the other",
        status=resp.status_code,
        served_by=received.get("instance_id"),
        gateway_instance_header=resp.headers.get("x-gateway-instance"),
    )
    assert resp.status_code == 200
    assert resp.headers.get("x-gateway-instance") == "users-v1-2"
    assert received["instance_id"] == "users-v1-2"
    # The stopped instance served nothing; the peer served exactly one.
    assert network.ups.meta("users-v1-2")["served_count"] == 1


def test_restarted_process_is_probed_back_without_business_traffic(network):
    from edge_gateway.prober import BackgroundProber

    network.ups.stop("users-v1-1")
    deadline = time.time() + 2.0
    while time.time() < deadline:
        try:
            httpx.get(network.ups.url("users-v1-1") + "/__health", timeout=0.2)
        except httpx.ConnectError:
            break
        time.sleep(0.02)
    for _ in range(3):
        network.client.get("/users/1", headers={"x-caller-id": "bob"})
    assert network.health.snapshot()["users-v1-1"]["state"] == "OPEN"

    # Restart the OS-level listener; no business request is sent meanwhile.
    network.ups.start("users-v1-1")
    prober = BackgroundProber(network.health, interval=0.02)
    prober.start()
    healed = False
    deadline = time.time() + 3.0
    while time.time() < deadline:
        if network.health.is_available("users-v1-1"):
            healed = True
            break
        time.sleep(0.02)
    prober.stop()

    events = network.health.events("users-v1-1")
    log_case(
        "restarted process heals via background probes with zero traffic",
        healed=healed,
        events=events,
    )
    assert healed
    assert [e["kind"] for e in events] == ["removed", "recovered"]


def test_non_idempotent_post_executes_at_most_once_on_5xx(network):
    network.ups.control("users-v1-1", {"mode": "error"})
    before = network.ups.meta("users-v1-2").get("served_count", 0)
    resp = network.client.post(
        "/users", content=b'{"id":1}', headers={"x-caller-id": "carol"}
    )
    body = resp.json()
    log_case(
        "POST 500 over real HTTP: never retried",
        status=resp.status_code,
        error=body["error"]["code"],
        instance=body["error"]["details"].get("instance_id"),
    )
    assert resp.status_code == 502
    assert body["error"]["code"] == "NON_IDEMPOTENT_NOT_RETRIED"
    assert body["error"]["details"]["instance_id"] == "users-v1-1"
    # The healthy peer must not have received the non-idempotent request.
    after = network.ups.meta("users-v1-2").get("served_count", 0)
    assert after == before
    assert network.ups.meta("users-v1-1")["served_count"] == 1


def test_non_idempotent_post_on_stopped_instance_fails_over_safely(network):
    # Cluster order is [users-v1-1, users-v1-2]: stop the first listener so
    # the connect fails before execution and the POST fails over to the peer.
    network.ups.stop("users-v1-1")
    deadline = time.time() + 2.0
    while time.time() < deadline:
        try:
            httpx.get(network.ups.url("users-v1-1") + "/__health", timeout=0.2)
        except httpx.ConnectError:
            break
        time.sleep(0.02)

    before = network.ups.meta("users-v1-2")["served_count"]
    resp = network.client.post(
        "/users", content=b'{"id":9}', headers={"x-caller-id": "carol"}
    )
    received = resp.json()
    log_case(
        "POST to a stopped process never executed -> safe failover to peer",
        status=resp.status_code,
        served_by=received.get("instance_id"),
        peer_delta=network.ups.meta("users-v1-2")["served_count"] - before,
    )
    assert resp.status_code == 200
    assert resp.headers.get("x-gateway-instance") == "users-v1-2"
    assert received["instance_id"] == "users-v1-2"
    assert network.ups.meta("users-v1-2")["served_count"] == before + 1


def test_event_loop_is_not_serialized_inflight_cap_holds(network):
    # Regression: the proxy handler must not block the asyncio event loop on
    # a synchronous upstream call. Make every orders... here users... no:
    # both cluster instances slow, then fire more than inflight_limit
    # concurrent requests through the ASGI app. If dispatch ran on the event
    # loop they would queue serially and all be accepted; with off-loop
    # dispatch exactly inflight_limit run and the rest are rejected at once.
    network.ups.control("users-v1-1", {"mode": "slow", "delay": 0.4})
    network.ups.control("users-v1-2", {"mode": "slow", "delay": 0.4})

    # users-read rule: inflight_limit=4, rate/quota generous enough for a
    # single 6-request burst on one caller.
    results: dict = {}
    lock = threading.Lock()

    def one(i):
        resp = network.client.get("/users/1", headers={"x-caller-id": "loop"})
        with lock:
            results[i] = (
                resp.status_code,
                resp.json().get("error", {}).get("code", "OK"),
            )

    threads = [threading.Thread(target=one, args=(i,)) for i in range(6)]
    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)
    elapsed = time.time() - start

    codes = [v[1] for _, v in sorted(results.items())]
    accepted = sum(1 for c in codes if c == "OK")
    rejected = sum(1 for c in codes if c == "INFLIGHT_LIMIT_EXCEEDED")
    log_case(
        "ASGI event loop not serialized: inflight cap holds concurrently",
        codes=codes,
        accepted=accepted,
        inflight_rejected=rejected,
        elapsed=round(elapsed, 2),
    )
    assert accepted == 4
    assert rejected == 2
    # All 6 finish in roughly one slow round-trip, not six serialized ones.
    assert elapsed < 1.2


def test_slow_upstream_times_out_and_non_idempotent_is_not_retried(network):
    # Longer than the registry timeout (0.6s).
    network.ups.control("users-v1-1", {"mode": "slow", "delay": 1.5})
    before = network.ups.meta("users-v1-2")["served_count"]
    start = time.time()
    resp = network.client.post(
        "/users", content=b'{"id":2}', headers={"x-caller-id": "carol"}
    )
    elapsed = time.time() - start
    body = resp.json()
    log_case(
        "slow upstream -> timeout -> non-idempotent not retried",
        elapsed=round(elapsed, 3),
        status=resp.status_code,
        error=body["error"]["code"],
        peer_served=network.ups.meta("users-v1-2")["served_count"] - before,
    )
    assert resp.status_code == 502
    assert body["error"]["code"] == "NON_IDEMPOTENT_NOT_RETRIED"
    assert elapsed < 1.2  # bounded by the configured timeout, not the 1.5s sleep
    assert network.ups.meta("users-v1-2")["served_count"] == before
