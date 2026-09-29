"""Drill environment: one-call bring-up, per-upstream process faults.

Uses the real :class:`DrillEnvironment` against a temporary work dir. The
gateway and all six upstreams are independent processes; this is slower than
the rest of the suite but proves the one-command local drill story.
"""

from __future__ import annotations

import json
import time
import urllib.request
import urllib.error
from pathlib import Path

import pytest

from edge_gateway.drill import GATEWAY_PORT, DRILL_TOPOLOGY, DrillEnvironment


def _get(url, headers=None, timeout=3.0):
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers)


@pytest.fixture()
def drill(tmp_path: Path):
    env = DrillEnvironment(workdir=str(tmp_path / "drill"))
    env.up()
    try:
        yield env
    finally:
        env.down()


def test_one_command_starts_gateway_and_all_upstreams(drill):
    status = drill.status()
    assert status["gateway"]["port_open"] is True
    for iid, info in status["upstreams"].items():
        assert info["started"] is True, iid
        assert info["port_open"] is True, iid
        assert info["healthy"] is True, iid
        assert info["remote"]["mode"] == "normal"

    # A real request traverses gateway -> upstream over TCP and echoes back.
    status_code, body, _headers = _get(
        f"http://127.0.0.1:{GATEWAY_PORT}/users/1",
        headers={"x-caller-id": "drill"},
    )
    echo = json.loads(body)
    assert status_code == 200
    assert echo["path"] == "/users/1"
    assert echo["headers"]["x-caller-id"] == "drill"


def test_stop_process_causes_failover_and_restart_recovers(drill):
    # Initially v1-1 serves (deterministic order).
    _code, body, headers = _get(
        f"http://127.0.0.1:{GATEWAY_PORT}/users/2",
        headers={"x-caller-id": "d2"},
    )
    assert headers["x-gateway-instance"] == "users-v1-1"

    # Kill the process for real.
    drill.stop_upstream("users-v1-1")
    status = drill.status()
    assert status["upstreams"]["users-v1-1"]["started"] is False
    assert status["upstreams"]["users-v1-1"]["port_open"] is False

    # Traffic now fails over to v1-2 after a real connection refusal.
    deadline = time.monotonic() + 5
    landed = set()
    while time.monotonic() < deadline:
        _c, _b, h = _get(
            f"http://127.0.0.1:{GATEWAY_PORT}/users/2",
            headers={"x-caller-id": "d3"},
        )
        landed.add(h.get("x-gateway-instance"))
        if "users-v1-2" in landed:
            break
        time.sleep(0.1)
    assert "users-v1-2" in landed

    # Restart the single upstream; background recovery (started inside the
    # gateway process) probes it back on its own.
    drill.start_upstream("users-v1-1")
    deadline = time.monotonic() + 10
    saw_v11 = False
    while time.monotonic() < deadline:
        _c, _b, h = _get(
            f"http://127.0.0.1:{GATEWAY_PORT}/users/2",
            headers={"x-caller-id": "d4"},
        )
        if h.get("x-gateway-instance") == "users-v1-1":
            saw_v11 = True
            break
        time.sleep(0.2)
    assert saw_v11, "restarted upstream never returned to rotation"


def test_slow_and_error_modes_over_the_network(drill):
    # Error mode: GET fails over past the 500.
    drill.set_mode("users-v1-1", "error")
    _code, body, headers = _get(
        f"http://127.0.0.1:{GATEWAY_PORT}/users/3",
        headers={"x-caller-id": "d5"},
    )
    assert headers["x-gateway-instance"] == "users-v1-2"
    assert drill.status()["upstreams"]["users-v1-1"]["remote"]["mode"] == "error"

    # Slow mode: a non-idempotent POST must time out at the gateway and never
    # be re-executed on the second instance.
    drill.set_mode("users-v1-1", "normal")
    drill.set_mode("users-v1-1", "slow", delay=5.0)
    req = urllib.request.Request(
        f"http://127.0.0.1:{GATEWAY_PORT}/users",
        data=b'{"id":9}',
        method="POST",
        headers={"x-caller-id": "d6", "content-type": "application/json"},
    )
    started = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            raise AssertionError(f"expected 502, got {resp.status}")
    except urllib.error.HTTPError as exc:
        payload = json.loads(exc.read())
        assert exc.code == 502
        assert payload["error"]["code"] == "NON_IDEMPOTENT_NOT_RETRIED"
    assert time.monotonic() - started < 4.0
    # The second instance never executed the POST.
    v2_calls = drill.status()["upstreams"]["users-v1-2"]["remote"]["calls"]
    assert v2_calls == 1  # only the earlier GET failover landed there

    drill.set_mode("users-v1-1", "normal")


def test_down_stops_everything(drill, tmp_path):
    drill.down()
    status = drill.status()
    assert status["gateway"] is None or status["gateway"]["alive"] is False
    for iid in DRILL_TOPOLOGY:
        assert status["upstreams"][iid]["started"] is False
    # State files removed.
    assert not list((tmp_path / "drill").glob("*.json"))
