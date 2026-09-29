"""Traffic-free background self-healing, audit events, single trip, no flap."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

from conftest import Harness, log_case, make_request
from edge_gateway.health import CLOSED, OPEN, HealthMonitor
from edge_gateway.prober import BackgroundProber
from edge_gateway.upstream import UpstreamError


def _wait_until(predicate, timeout=2.0, interval=0.01):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def test_instance_self_heals_with_zero_business_traffic():
    # probe_interval=0 so recovery proceeds as fast as the background loop runs.
    harness = Harness(failure_threshold=3, recovery_threshold=2, probe_interval=0.0)
    harness.registry.set_fault("users-v1-1", "down")
    for _ in range(3):
        harness.handle("GET", "/users/1")
    assert harness.health.snapshot()["users-v1-1"]["state"] == OPEN

    # Upstream comes back, but nobody sends the gateway any business request.
    harness.registry.set_fault("users-v1-1", None)
    prober = BackgroundProber(harness.health, interval=0.01)
    prober.start()
    try:
        healed = _wait_until(lambda: harness.health.is_available("users-v1-1"))
    finally:
        prober.stop()

    events = harness.health.events("users-v1-1")
    log_case(
        "no traffic: background probing brings the instance back",
        make_request("GET", "/users/1"),
        healed=healed,
        state=harness.health.snapshot()["users-v1-1"]["state"],
        events=events,
    )
    assert healed
    assert harness.health.snapshot()["users-v1-1"]["state"] == CLOSED
    kinds = [e["kind"] for e in events]
    assert kinds == ["removed", "recovered"]


def test_removal_and_recovery_events_carry_time_and_reason():
    harness = Harness(failure_threshold=2, recovery_threshold=1, probe_interval=0.0)
    harness.registry.set_fault("orders-v1-1", "down")
    harness.handle("GET", "/orders/1")
    harness.handle("GET", "/orders/1")

    # From the recovery probes' perspective the instance is healthy now.
    healthy = _ScriptedRegistry([True])
    harness.health._registry = healthy  # noqa: SLF001

    prober = BackgroundProber(harness.health, interval=0.01)
    prober.start()
    try:
        _wait_until(lambda: harness.health.is_available("orders-v1-1"))
    finally:
        prober.stop()

    events = harness.health.events("orders-v1-1")
    log_case(
        "audit events record removal and recovery with time + reason",
        make_request("GET", "/orders/1"),
        events=events,
    )
    removed, recovered = events[0], events[1]
    assert removed["kind"] == "removed"
    assert isinstance(removed["timestamp"], float)
    assert removed["reason"]  # a concrete reason from the failing dispatch
    assert "unreachable" in removed["reason"]
    assert recovered["kind"] == "recovered"
    assert isinstance(recovered["timestamp"], float)
    assert recovered["timestamp"] >= removed["timestamp"]
    assert "probe success" in recovered["reason"]

    snap = harness.health.snapshot()["orders-v1-1"]
    assert snap["last_removed_reason"] == removed["reason"]
    assert snap["last_recovered_reason"] == recovered["reason"]


def test_concurrent_failures_emit_a_single_removal_event():
    harness = Harness(failure_threshold=3, recovery_threshold=2, probe_interval=10.0)

    def fail_once(_):
        harness.health.record_failure("users-v1-1", "concurrent test failure")

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(fail_once, range(50)))

    removals = [
        e for e in harness.health.events("users-v1-1") if e["kind"] == "removed"
    ]
    log_case(
        "50 concurrent failure reports trip the instance exactly once",
        make_request("GET", "/users/1"),
        removal_events=removals,
        state=harness.health.snapshot()["users-v1-1"]["state"],
    )
    assert len(removals) == 1
    assert harness.health.snapshot()["users-v1-1"]["state"] == OPEN


class _ScriptedRegistry:
    """Probe results follow a script; business calls are never made."""

    def __init__(self, results: list[bool]):
        self._results = list(results)
        self.calls = 0

    def probe(self, instance_id: str) -> None:
        i = min(self.calls, len(self._results) - 1)
        self.calls += 1
        if not self._results[i]:
            raise UpstreamError("scripted failure")


def test_recovery_needs_a_streak_and_a_failure_resets_it():
    # fail, success, success (still OPEN), fail (streak reset), then 3
    # consecutive successes -> CLOSED only on the third.
    registry = _ScriptedRegistry([False, True, True, False, True, True, True])
    health = HealthMonitor(
        ["x"], registry, failure_threshold=1, recovery_threshold=3, probe_interval=0.0
    )
    health.record_failure("x", "seed trip")
    assert health.snapshot()["x"]["state"] == OPEN

    def one_probe():
        claimed = health.due_probes()
        assert claimed == ["x"]
        return health.probe("x")

    one_probe()  # 1: probe fails -> streak reset, stays OPEN
    assert health.snapshot()["x"]["state"] == OPEN
    assert one_probe() is False  # 2: success 1
    assert health.snapshot()["x"]["state"] == OPEN
    assert one_probe() is False  # 3: success 2, not yet
    assert health.snapshot()["x"]["state"] == OPEN
    assert one_probe() is False  # 4: failure resets streak
    assert health.snapshot()["x"]["state"] == OPEN
    flags = []
    flags.append(one_probe())  # 5: success 1
    flags.append(one_probe())  # 6: success 2
    flags.append(one_probe())  # 7: success 3 -> CLOSED

    final = health.snapshot()["x"]["state"]
    log_case(
        "a single lucky probe never reopens; streak must be consecutive",
        make_request("GET", "/x"),
        reopened_flags=flags,
        recovery_successes=health.snapshot()["x"]["recovery_successes"],
        final_state=final,
    )
    assert flags == [False, False, True]
    assert final == CLOSED


def test_background_prober_lifecycle():
    harness = Harness(probe_interval=0.0)
    prober = BackgroundProber(harness.health, interval=0.01)
    assert prober.running is False
    prober.start()
    assert prober.running is True
    prober.start()  # idempotent: does not spawn a second thread
    prober.stop()
    log_case(
        "background prober starts once and stops promptly",
        make_request("GET", "/users/1"),
        running_after_stop=prober.running,
    )
    assert prober.running is False
