"""HTTP surface of network mode: transparent passthrough + real failover.

The FastAPI app talks to real upstream processes; successful upstream
responses must reach the caller with their original status/body, and the
background recovery loop must restore a restarted process without traffic.
"""

from __future__ import annotations

import json
import socket
import subprocess
import sys
import time

import pytest
from fastapi.testclient import TestClient

from edge_gateway.app import create_network_app
from edge_gateway.realupstream import HttpUpstreamRegistry
from edge_gateway.upstream import UpstreamError


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


@pytest.fixture()
def client():
    clusters_instances = ["users-v1-1", "users-v1-2"]
    ports = {iid: _free_port() for iid in clusters_instances}
    procs = {}
    for iid in clusters_instances:
        procs[iid] = subprocess.Popen(
            [sys.executable, "-m", "edge_gateway.realupstream",
             "--id", iid, "--port", str(ports[iid])],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

    def _kill_all() -> None:
        for proc in procs.values():
            proc.terminate()
        for proc in procs.values():
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()

    base_urls = {iid: f"http://127.0.0.1:{ports[iid]}" for iid in clusters_instances}
    recovery = None
    try:
        app = create_network_app(
            base_urls=base_urls,
            start_recovery=False,
            clusters={"users-v1": clusters_instances},
        )
        # Tune the in-app components for a fast test.
        gateway = app.state.gateway
        gateway._health._failure_threshold = 1  # noqa: SLF001
        gateway._health._recovery_threshold = 1  # noqa: SLF001
        gateway._health._probe_interval = 0.1  # noqa: SLF001
        from edge_gateway.recovery import RecoveryLoop

        recovery = RecoveryLoop(gateway._health, interval=0.05)  # noqa: SLF001
        app.state.recovery = recovery
        recovery.start()

        registry = HttpUpstreamRegistry()
        for iid in clusters_instances:
            registry.register(iid, base_urls[iid], "users-v1")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                for iid in clusters_instances:
                    registry.probe(iid)
                break
            except UpstreamError:
                time.sleep(0.05)
        else:
            raise AssertionError("upstreams never came up")
    except BaseException:
        if recovery is not None:
            recovery.stop()
        _kill_all()
        raise

    with TestClient(app) as c:
        c._drill = {"procs": procs, "ports": ports, "registry": registry,
                    "recovery": recovery}
        yield c

    recovery.stop()
    _kill_all()


def test_transparent_response_passes_upstream_body_and_status(client):
    resp = client.get("/users/9", headers={"x-caller-id": "bob"})
    assert resp.status_code == 200
    echo = resp.json()
    assert echo["method"] == "GET"
    assert echo["path"] == "/users/9"
    assert resp.headers["x-gateway-rule"] == "users-read"
    assert resp.headers["x-served-by"].startswith("users-v1")


def test_post_body_and_query_passthrough(client):
    resp = client.post(
        "/users?a=1&b=two",
        headers={"x-caller-id": "carol", "content-type": "application/json"},
        content=b'{"k":"v"}',
    )
    assert resp.status_code == 200
    echo = resp.json()
    assert echo["method"] == "POST"
    assert echo["query"] == "a=1&b=two"
    assert echo["body_text"] == '{"k":"v"}'
    assert echo["headers"]["content-type"] == "application/json"


def test_process_stop_failover_and_background_recovery(client):
    drill = client._drill
    # Stop users-v1-1: first GET trips (threshold 1) and fails over.
    drill["procs"]["users-v1-1"].terminate()
    drill["procs"]["users-v1-1"].wait(timeout=3)
    resp = client.get("/users/1", headers={"x-caller-id": "z1"})
    assert resp.status_code == 200
    assert resp.headers["x-gateway-instance"] == "users-v1-2"

    state = client.get("/__admin/state").json()
    assert state["health"]["users-v1-1"]["state"] == "OPEN"

    # Restart the process; the background loop must restore it with NO
    # further requests sent through the gateway.
    new_proc = subprocess.Popen(
        [sys.executable, "-m", "edge_gateway.realupstream",
         "--id", "users-v1-1", "--port", str(drill["ports"]["users-v1-1"])],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    drill["procs"]["users-v1-1"] = new_proc
    try:
        deadline = time.monotonic() + 8
        healed = False
        while time.monotonic() < deadline:
            st = client.get("/__admin/state").json()["health"]["users-v1-1"]
            if st["state"] == "CLOSED":
                healed = True
                break
            time.sleep(0.1)
        assert healed
        events = st["events"]
        assert [e["kind"] for e in events].count("removed") == 1
        assert any(e["kind"] == "recovered" for e in events)
    finally:
        new_proc.terminate()
        new_proc.wait(timeout=3)


def test_admin_error_mode_returns_5xx_passthrough_for_get_failover(client):
    drill = client._drill
    drill["registry"].set_remote_mode("users-v1-1", "error")
    resp = client.get("/users/2", headers={"x-caller-id": "z2"})
    # GET fails over to healthy v1-2.
    assert resp.status_code == 200
    assert resp.headers["x-gateway-instance"] == "users-v1-2"
    drill["registry"].set_remote_mode("users-v1-1", "normal")
