"""Background self-healing without traffic, and removal/recovery records."""

from __future__ import annotations

import threading
import time

from conftest import Harness, log_case, make_request


def _trip(harness, instance_id="users-v1-1", path="/users/1"):
    harness.registry.set_fault(instance_id, "down")
    for _ in range(3):
        harness.handle("GET", path)
    assert harness.health.snapshot()[instance_id]["state"] == "OPEN"


def test_self_heals_with_zero_traffic():
    # Recovery runs on the injected clock with probe_interval=0 so every
    # background run is "due"; no user request happens after the trip.
    clock = {"t": 0.0}
    harness = Harness(probe_interval=0.0)
    harness.health._time = lambda: clock["t"]  # noqa: SLF001
    from edge_gateway.recovery import RecoveryLoop

    recovery = RecoveryLoop(harness.health, interval=0.005)

    _trip(harness)
    harness.registry.set_fault("users-v1-1", None)

    # NO traffic: only the background loop drives probes.
    recovery.start()
    try:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if harness.health.is_available("users-v1-1"):
                break
            time.sleep(0.02)
        state = harness.health.snapshot()["users-v1-1"]
        log_case(
            "instance recovered with zero user traffic",
            make_request("GET", "/users/1"),
            state=state["state"],
            recovery_successes=state["recovery_successes"],
        )
        assert harness.health.is_available("users-v1-1")
    finally:
        recovery.stop()


def test_removal_and_recovery_events_are_recorded_with_time_and_reason():
    clock = {"t": 0.0}
    wall = {"t": 1000.0}
    harness = Harness(probe_interval=0.0)
    # Rebuild health with a controllable wall clock for stable assertions.
    from edge_gateway.health import HealthMonitor
    from edge_gateway.recovery import RecoveryLoop

    instances = [i for ids in harness.clusters.values() for i in ids]
    health = HealthMonitor(
        instances,
        harness.registry,
        failure_threshold=3,
        recovery_threshold=2,
        probe_interval=0.0,
        time_func=lambda: clock["t"],
        wall_clock=lambda: wall["t"],
    )
    harness.gateway._health = health  # noqa: SLF001
    harness.health = health

    _trip(harness)
    removed = health.events("users-v1-1")
    log_case("removal event", event=removed)
    assert len(removed) == 1
    assert removed[0]["kind"] == "removed"
    assert removed[0]["time"] == 1000.0
    assert removed[0]["reason"]
    assert removed[0]["consecutive_failures"] == 3

    # Background recovery: two consecutive successful probes (threshold 2).
    harness.registry.set_fault("users-v1-1", None)
    recovery = RecoveryLoop(health, interval=0.005)
    recovery.start()
    try:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if any(e["kind"] == "recovered" for e in health.events("users-v1-1")):
                break
            time.sleep(0.02)
    finally:
        recovery.stop()

    kinds = [e["kind"] for e in health.events("users-v1-1")]
    recovered = [e for e in health.events("users-v1-1") if e["kind"] == "recovered"]
    log_case("full event history", kinds=kinds, recovered=recovered)
    assert kinds[0] == "removed"
    assert kinds[-1] == "recovered"
    assert recovered[0]["reason"]
    assert recovered[0]["time"] >= removed[0]["time"]


def test_single_lucky_probe_does_not_recover_even_in_background():
    # Probe succeeds once, then the upstream fails again: streak resets and
    # the instance must stay out of rotation.
    harness = Harness(probe_interval=0.0, recovery_threshold=2)
    from edge_gateway.recovery import RecoveryLoop

    _trip(harness)

    # One good probe via the background loop machinery.
    harness.registry.set_fault("users-v1-1", None)
    loop = RecoveryLoop(harness.health, interval=0.005)
    assert loop.run_once()  # claims and runs the first probe
    assert harness.health.snapshot()["users-v1-1"]["state"] == "OPEN"

    # Goes bad again: next background probe fails and resets the streak.
    harness.registry.set_fault("users-v1-1", "down")
    loop.run_once()  # interval is 0, probe due immediately
    state = harness.health.snapshot()["users-v1-1"]
    events = harness.health.events("users-v1-1")
    log_case(
        "flaky instance stays out after one lucky probe",
        state=state["state"],
        streak=state["recovery_successes"],
        event_kinds=[e["kind"] for e in events],
    )
    assert state["state"] == "OPEN"
    assert state["recovery_successes"] == 0
    assert any(e["kind"] == "probe_failed" for e in events)


def test_background_and_traffic_probes_never_double_claim():
    # While the background loop runs, opportunistic traffic-triggered claims
    # must never result in two probes of the same instance at once.
    harness = Harness(probe_interval=0.0, recovery_threshold=5)
    from edge_gateway.recovery import RecoveryLoop

    _trip(harness)
    harness.registry.set_fault("users-v1-1", None)

    claims: list[str] = []
    lock = threading.Lock()
    loop = RecoveryLoop(harness.health, interval=0.005)

    original_probe = harness.health.probe

    def tracked_probe(instance_id):
        with lock:
            claims.append(instance_id)
        time.sleep(0.005)  # widen the race window
        return original_probe(instance_id)

    harness.health.probe = tracked_probe  # type: ignore[method-assign]

    loop.start()
    stop = threading.Event()
    try:
        # Traffic concurrently asks for due probes too, long enough for the
        # background loop to complete the full recovery streak.
        def traffic_claim():
            while not stop.is_set():
                for iid in harness.health.due_probes():
                    harness.health.probe(iid)
                stop.wait(0.002)

        t = threading.Thread(target=traffic_claim)
        t.start()
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not harness.health.is_available(
            "users-v1-1"
        ):
            time.sleep(0.01)
        stop.set()
        t.join(2)
    finally:
        loop.stop()

    # Recovery threshold is 5: exactly 5 successful probes bring it back;
    # the atomic claim makes duplicate in-flight probes impossible.
    probes = [c for c in claims if c == "users-v1-1"]
    log_case(
        "background + traffic probing stays deduplicated",
        probe_count=len(probes),
        final_state=harness.health.snapshot()["users-v1-1"]["state"],
    )
    assert harness.health.is_available("users-v1-1")
    assert len(probes) == 5
