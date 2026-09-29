"""End-to-end behavior against REAL upstream processes over TCP.

These tests spawn each upstream as an independent OS process
(``python -m edge_gateway.realupstream``) and build a gateway in network
mode with a short request timeout. They cover:

* verbatim forwarding of method/path/query/body/common headers, and verbatim
  return of upstream status + body;
* process stop -> connection refused -> failover; process restart -> the
  health monitor probes it back into rotation;
* slow response -> real socket timeout, non-idempotent request is NOT
  retried;
* error response -> real HTTP 5xx semantics.
"""

from __future__ import annotations

import json
import socket
import subprocess
import sys
import time

import pytest

from edge_gateway.config import default_rules
from edge_gateway.errors import (
    AllUpstreamsUnavailableError,
    NonIdempotentRetryError,
)
from edge_gateway.gateway import Gateway
from edge_gateway.health import HealthMonitor
from edge_gateway.admission import AdmissionManager
from edge_gateway.realupstream import HttpUpstreamRegistry
from edge_gateway.router import Router, RuleRegistry
from edge_gateway.upstream import UpstreamError


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


@pytest.fixture()
def network_env():
    """Start two users-v1 upstream processes + a network-mode gateway."""

    clusters = {"users-v1": ["users-v1-1", "users-v1-2"]}
    # Only run the users-v1 pair to keep this suite fast.
    instances = ["users-v1-1", "users-v1-2"]
    ports = {iid: _free_port() for iid in instances}
    procs = {}
    for iid in instances:
        procs[iid] = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "edge_gateway.realupstream",
                "--id",
                iid,
                "--port",
                str(ports[iid]),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    registry = HttpUpstreamRegistry(request_timeout=1.0, probe_timeout=0.5)
    for iid in instances:
        registry.register(iid, f"http://127.0.0.1:{ports[iid]}", "users-v1")

    def _kill_all() -> None:
        for proc in procs.values():
            proc.terminate()
        for proc in procs.values():
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()

    # Fast trip (1 failure) and a single confirming probe so the tests are
    # quick while still exercising the real state machine.
    health = HealthMonitor(
        instances,
        registry,
        failure_threshold=1,
        recovery_threshold=1,
        probe_interval=0.2,
    )
    # Wait for both processes to accept connections. If setup fails after the
    # processes spawned, tear them down so nothing is orphaned.
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                for iid in instances:
                    registry.probe(iid)
                break
            except UpstreamError:
                time.sleep(0.05)
        else:
            raise AssertionError("upstreams never came up")

        gateway = Gateway(
            router=Router(RuleRegistry(default_rules())),
            quotas=AdmissionManager(),
            health=health,
            registry=registry,
            clusters=clusters,
            transparent=True,
        )
    except BaseException:
        _kill_all()
        raise
    from edge_gateway.models import RequestContext

    def request(method="GET", path="/users/1", caller="net", **kw):
        headers = {k.lower(): v for k, v in (kw.pop("headers", None) or {}).items()}
        headers.setdefault("x-caller-id", caller)
        return RequestContext(method, path, headers, kw.pop("query", {}), caller,
                              kw.pop("body", b""))

    try:
        yield {
            "gateway": gateway,
            "registry": registry,
            "health": health,
            "procs": procs,
            "ports": ports,
            "request": request,
            "instances": instances,
        }
    finally:
        _kill_all()


def test_method_path_query_headers_and_body_forwarded_verbatim(network_env):
    gw = network_env["gateway"]
    req = network_env["request"](
        "POST",
        "/users/7?x=1",
        caller="alice",
        headers={"x-custom": "hello", "x-canary": "false"},
        body=b'{"name":"simon"}',
    )
    result = gw.handle(req)
    echo = json.loads(result.raw_body)
    assert result.upstream_status == 200
    assert echo["method"] == "POST"
    assert echo["path"] == "/users/7"
    assert echo["query"] == "x=1"
    assert echo["body_text"] == '{"name":"simon"}'
    assert echo["headers"]["x-custom"] == "hello"
    assert echo["headers"]["x-caller-id"] == "alice"
    assert result.transparent is True


def test_upstream_5xx_status_and_body_returned_to_caller(network_env):
    env = network_env
    env["registry"].set_remote_mode("users-v1-1", "error")
    # GET is idempotent: fail over past the 500 to the healthy instance.
    result = env["gateway"].handle(env["request"]("GET", "/users/1", caller="e1"))
    assert result.instance_id == "users-v1-2"
    assert [a.outcome for a in result.attempts] == ["http_5xx", "success"]
    assert result.upstream_status == 200
    env["registry"].set_remote_mode("users-v1-1", "normal")


def test_stopped_process_causes_real_failover(network_env):
    env = network_env
    env["procs"]["users-v1-1"].terminate()
    env["procs"]["users-v1-1"].wait(timeout=3)
    result = env["gateway"].handle(env["request"]("GET", "/users/1", caller="down1"))
    assert result.instance_id == "users-v1-2"
    assert result.attempts[0].outcome == "unreachable"
    assert "refused" in result.attempts[0].detail.lower() or "unreachable" in (
        result.attempts[0].detail.lower()
    )


def test_all_processes_down_returns_unavailable(network_env):
    env = network_env
    for iid in env["instances"]:
        env["procs"][iid].terminate()
    with pytest.raises(AllUpstreamsUnavailableError):
        env["gateway"].handle(env["request"]("GET", "/users/1", caller="alldown"))


def test_non_idempotent_post_not_retried_on_timeout(network_env):
    env = network_env
    # users-v1-1 slow (> gateway request timeout of 1s).
    env["registry"].set_remote_mode("users-v1-1", "slow", delay=1.5)
    started = time.monotonic()
    with pytest.raises(NonIdempotentRetryError):
        env["gateway"].handle(
            env["request"](
                "POST", "/users", caller="slow1", body=b'{"id":1}'
            )
        )
    elapsed = time.monotonic() - started
    # The slow instance executed the POST (unconfirmed) -> v1-2 was never tried.
    state = env["registry"].remote_state("users-v1-2")
    assert state["calls"] == 0
    assert elapsed < 3.0  # failed fast at the socket timeout


def test_restarted_process_is_probed_back_into_rotation(network_env):
    env = network_env
    proc = env["procs"]["users-v1-1"]
    proc.terminate()
    proc.wait(timeout=3)
    # Drive enough traffic to trip it (failure_threshold=1).
    for _ in range(2):
        result = env["gateway"].handle(
            env["request"]("GET", "/users/1", caller="trip1")
        )
        assert result.instance_id == "users-v1-2"
    assert env["health"].snapshot()["users-v1-1"]["state"] == "OPEN"
    removed = env["health"].events("users-v1-1")
    assert any(e["kind"] == "removed" for e in removed)

    # Restart the SAME instance on its fixed port as a fresh process.
    new_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "edge_gateway.realupstream",
            "--id",
            "users-v1-1",
            "--port",
            str(env["ports"]["users-v1-1"]),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    env["procs"]["users-v1-1"] = new_proc
    try:
        deadline = time.monotonic() + 8
        back = False
        while time.monotonic() < deadline:
            # Opportunistic probing happens during request handling.
            result = env["gateway"].handle(
                env["request"]("GET", "/users/1", caller="heal1")
            )
            if result.instance_id == "users-v1-1":
                back = True
                break
            time.sleep(0.1)
        assert back, "restarted instance never returned to rotation"
        events = env["health"].events("users-v1-1")
        assert any(e["kind"] == "recovered" for e in events)
    finally:
        new_proc.terminate()
        new_proc.wait(timeout=3)


def test_real_process_self_heals_without_traffic(network_env):
    """Stop a real process, restart it, and let ONLY the background loop
    restore it - no gateway requests after the restart."""

    from edge_gateway.recovery import RecoveryLoop

    env = network_env
    recovery = RecoveryLoop(env["health"], interval=0.1)

    proc = env["procs"]["users-v1-1"]
    proc.terminate()
    proc.wait(timeout=3)
    # Trip it with traffic.
    env["gateway"].handle(env["request"]("GET", "/users/1", caller="ztrip"))
    assert env["health"].snapshot()["users-v1-1"]["state"] == "OPEN"

    # Restart as a fresh process.
    new_proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "edge_gateway.realupstream",
            "--id",
            "users-v1-1",
            "--port",
            str(env["ports"]["users-v1-1"]),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    env["procs"]["users-v1-1"] = new_proc
    recovery.start()
    try:
        deadline = time.monotonic() + 8
        healed = False
        while time.monotonic() < deadline:
            if env["health"].is_available("users-v1-1"):
                healed = True
                break
            time.sleep(0.1)
        # NO gateway.handle() call happened in this window: only the
        # background RecoveryLoop probed the restarted process.
        assert healed, "background loop did not restore the restarted process"
        events = env["health"].events("users-v1-1")
        kinds = [e["kind"] for e in events]
        assert "removed" in kinds and "recovered" in kinds
    finally:
        recovery.stop()
        new_proc.terminate()
        new_proc.wait(timeout=3)
