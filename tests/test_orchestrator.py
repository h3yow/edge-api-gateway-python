"""End-to-end: the orchestrator runs upstreams as real OS processes that can
each be started and stopped independently. The gateway is driven in-process
against those processes' real loopback ports.
"""

from __future__ import annotations

import socket
import time

import httpx
import pytest

from conftest import log_case
from edge_gateway.admission import AdmissionManager
from edge_gateway.gateway import Gateway
from edge_gateway.health import HealthMonitor
from edge_gateway.http_upstream import HttpUpstreamRegistry
from edge_gateway.lab import lab_config
from edge_gateway.lab.netapp import create_network_app
from edge_gateway.lab.orchestrator import LabOrchestrator
from edge_gateway.prober import BackgroundProber
from edge_gateway.router import Router, RuleRegistry
from fastapi.testclient import TestClient


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture()
def proc_lab(tmp_path):
    instances = ["users-v1-1", "users-v1-2"]
    # Unique free ports, injected into a minimal orchestrator by rebinding.
    orch = LabOrchestrator(state_dir=str(tmp_path))
    ports = {i: _free_port() for i in instances}
    orch._ports = ports  # noqa: SLF001

    for instance_id in instances:
        orch.start_instance(instance_id)

    clusters = {"users-v1": instances}
    rules = [r for r in lab_config.default_rules() if r.id in ("users-read", "users-write")]
    registry = HttpUpstreamRegistry(lab_config.base_urls(ports), timeout=1.0)
    health = HealthMonitor(instances, registry, failure_threshold=3,
                           recovery_threshold=2, probe_interval=0.05)
    gateway = Gateway(
        router=Router(RuleRegistry(rules)),
        quotas=AdmissionManager(),
        health=health,
        registry=registry,
        clusters=clusters,
    )
    app = create_network_app(gateway, prober_interval=0.02, start_prober=False)
    client = TestClient(app, raise_server_exceptions=False)
    prober = BackgroundProber(health, interval=0.02)
    prober.start()
    try:
        yield type("Env", (), {
            "orch": orch, "client": client, "ports": ports,
            "registry": registry, "health": health, "instances": instances,
        })()
    finally:
        prober.stop()
        client.close()
        registry.close()
        for instance_id in instances:
            orch.stop_instance(instance_id)


def test_orchestrator_starts_independent_processes(proc_lab):
    status = proc_lab.orch.status()
    log_case(
        "each upstream is a real, independently tracked OS process",
        instances={i: status["instances"][i]["alive"] for i in proc_lab.instances},
    )
    assert all(status["instances"][i]["alive"] for i in proc_lab.instances)
    resp = proc_lab.client.get("/users/1", headers={"x-caller-id": "bob"})
    assert resp.status_code == 200
    assert resp.headers.get("x-gateway-instance") == "users-v1-1"


def test_stop_one_process_then_restart_and_self_heal(proc_lab):
    proc_lab.orch.stop_instance("users-v1-1")
    deadline = time.time() + 3.0
    url = f"http://127.0.0.1:{proc_lab.ports['users-v1-1']}/__health"
    while time.time() < deadline:
        try:
            httpx.get(url, timeout=0.2)
        except httpx.ConnectError:
            break
        time.sleep(0.03)

    for _ in range(3):
        resp = proc_lab.client.get("/users/1", headers={"x-caller-id": "bob"})
        assert resp.headers.get("x-gateway-instance") == "users-v1-2"
    assert proc_lab.health.snapshot()["users-v1-1"]["state"] == "OPEN"

    # Independent restart of just that one process.
    proc_lab.orch.start_instance("users-v1-1")
    healed = False
    deadline = time.time() + 4.0
    while time.time() < deadline:
        if proc_lab.health.is_available("users-v1-1"):
            healed = True
            break
        time.sleep(0.03)

    resp = proc_lab.client.get("/users/1", headers={"x-caller-id": "bob"})
    log_case(
        "one process stopped then independently restarted; traffic resumes",
        healed=healed,
        served_by=resp.headers.get("x-gateway-instance"),
        events=proc_lab.health.events("users-v1-1"),
    )
    assert healed
    assert resp.headers.get("x-gateway-instance") == "users-v1-1"


def test_slow_and_error_modes_via_orchestrator(proc_lab):
    # error mode: 500 from the first, GET fails over to the healthy peer.
    proc_lab.orch.set_mode("users-v1-1", "error")
    resp = proc_lab.client.get("/users/1", headers={"x-caller-id": "bob"})
    log_case("error mode: 500 instance is skipped", served_by=resp.headers.get("x-gateway-instance"))
    assert resp.headers.get("x-gateway-instance") == "users-v1-2"

    # back to normal, then slow mode on the serving instance (GET times out
    # but the idempotent GET still fails over to a healthy peer).
    proc_lab.orch.set_mode("users-v1-1", "normal")
    proc_lab.orch.set_mode("users-v1-2", "slow", delay=1.5)
    start = time.time()
    resp = proc_lab.client.get("/users/1", headers={"x-caller-id": "bob"})
    elapsed = time.time() - start
    log_case(
        "slow mode: idempotent GET times out then fails over",
        served_by=resp.headers.get("x-gateway-instance"),
        elapsed=round(elapsed, 2),
    )
    assert resp.headers.get("x-gateway-instance") == "users-v1-1"
    assert elapsed < 2.5
    proc_lab.orch.set_mode("users-v1-2", "normal")
