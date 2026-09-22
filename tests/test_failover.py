"""Health probing, failover and non-idempotent safety under concurrency."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from conftest import Harness, log_case, make_request
from edge_gateway.errors import (
    AllUpstreamsUnavailableError,
    NonIdempotentRetryError,
)


def test_idempotent_request_fails_over_and_second_attempt_succeeds(harness):
    harness.registry.set_fault("users-v1-1", "fail")
    req = make_request("GET", "/users/1")
    result = harness.handle("GET", "/users/1")
    log_case(
        "GET retries past a failing instance",
        req,
        attempts=[(a.instance_id, a.outcome) for a in result.attempts],
        final=result.instance_id,
        trace=result.trace,
    )
    assert [a.outcome for a in result.attempts] == ["http_5xx", "success"]
    assert result.instance_id == "users-v1-2"


def test_repeated_failures_trip_instance_once(harness):
    harness.registry.set_fault("users-v1-1", "down")
    trips = 0
    for i in range(5):
        # v1-1 unreachable, failover to v1-2 succeeds.
        result = harness.handle("GET", "/users/1")
        assert result.instance_id == "users-v1-2"
    snap = harness.health.snapshot()["users-v1-1"]
    log_case(
        "consecutive failures remove instance exactly once",
        make_request("GET", "/users/1"),
        health=snap,
    )
    assert snap["state"] == "OPEN"
    assert snap["consecutive_failures"] >= 3


def test_all_instances_down_returns_distinguishable_error(harness):
    for instance_id in ("orders-v1-1", "orders-v1-2"):
        harness.registry.set_fault(instance_id, "down")
    with pytest.raises(AllUpstreamsUnavailableError) as exc:
        harness.handle("GET", "/orders/1")
    log_case(
        "no healthy upstream -> explicit error, never random pick",
        make_request("GET", "/orders/1"),
        error=exc.value.code,
        details=exc.value.details["instances"],
    )
    assert exc.value.status_code == 503


def test_non_idempotent_post_executes_at_most_once_on_5xx(harness):
    harness.registry.set_fault("users-v1-1", "fail")
    req = make_request("POST", "/users", caller="carol", body=b'{"id":1}')
    with pytest.raises(NonIdempotentRetryError) as exc:
        harness.gateway.handle(req)
    log_case(
        "POST that 500s is not failed over (already executed once)",
        req,
        error=exc.value.code,
        executions={
            "users-v1-1": harness.registry.call_count("users-v1-1"),
            "users-v1-2": harness.registry.call_count("users-v1-2"),
        },
        trace=exc.value.details.get("attempts"),
    )
    assert harness.registry.call_count("users-v1-1") == 1
    assert harness.registry.call_count("users-v1-2") == 0


def test_non_idempotent_post_on_connection_error_may_failover_safely(harness):
    # 'down' means the handler never executed: retry cannot double-run it.
    harness.registry.set_fault("users-v1-1", "down")
    req = make_request("POST", "/users", caller="carol", body=b'{"id":2}')
    result = harness.gateway.handle(req)
    log_case(
        "POST to an unreachable instance may safely fail over",
        req,
        attempts=[(a.instance_id, a.outcome) for a in result.attempts],
        final=result.instance_id,
    )
    assert harness.registry.call_count("users-v1-1") == 0
    assert harness.registry.call_count("users-v1-2") == 1
    assert result.instance_id == "users-v1-2"


def test_non_idempotent_timeout_is_not_retried(harness):
    harness.registry.set_fault("users-v1-1", "timeout")
    req = make_request("POST", "/users", caller="carol", body=b'{"id":3}')
    with pytest.raises(NonIdempotentRetryError):
        harness.gateway.handle(req)
    log_case(
        "POST timeout: execution unconfirmed -> no second execution",
        req,
        executions={
            "users-v1-1": harness.registry.call_count("users-v1-1"),
            "users-v1-2": harness.registry.call_count("users-v1-2"),
        },
    )
    assert harness.registry.call_count("users-v1-2") == 0


def test_recovery_requires_consecutive_successful_probes(harness):
    harness.registry.set_fault("users-v1-1", "down")
    for _ in range(3):
        harness.handle("GET", "/users/1")
    assert harness.health.snapshot()["users-v1-1"]["state"] == "OPEN"

    # While one probe is in flight, concurrent claims get nothing.
    harness.registry.set_fault("users-v1-1", None)
    assert harness.health.due_probes() == ["users-v1-1"]
    assert harness.health.due_probes() == []  # already PROBING, claimed once
    first = harness.health.probe("users-v1-1")
    state_after_one = harness.health.snapshot()["users-v1-1"]["state"]
    # One successful probe is not enough (anti-flapping).
    harness.health.due_probes()
    second = harness.health.probe("users-v1-1")
    final = harness.health.snapshot()["users-v1-1"]["state"]
    log_case(
        "recovery needs a probe streak, single probe keeps it out",
        make_request("GET", "/users/1"),
        first_probe_reopened=first,
        state_after_one_probe=state_after_one,
        second_probe_reopened=second,
        final_state=final,
    )
    assert first is False
    assert state_after_one == "OPEN"
    assert second is True
    assert final == "CLOSED"


def test_failed_probe_resets_recovery_streak(harness):
    harness.registry.set_fault("users-v1-1", "down")
    for _ in range(3):
        harness.handle("GET", "/users/1")
    harness.registry.set_fault("users-v1-1", None)
    harness.health.due_probes()
    assert harness.health.probe("users-v1-1") is False  # one good probe
    # Goes bad again before the second confirming probe.
    harness.registry.set_fault("users-v1-1", "down")
    harness.health.due_probes()
    assert harness.health.probe("users-v1-1") is False
    streak = harness.health.snapshot()["users-v1-1"]["recovery_successes"]
    log_case(
        "flaky upstream: one bad probe resets the recovery streak",
        make_request("GET", "/users/1"),
        recovery_successes=streak,
        state=harness.health.snapshot()["users-v1-1"]["state"],
    )
    assert streak == 0
    assert harness.health.snapshot()["users-v1-1"]["state"] == "OPEN"


def test_concurrent_probes_never_duplicated():
    # Positive probe interval: after one probe the instance must not be
    # probe-due again immediately, so claims under concurrency are unique.
    import time as _time

    harness = Harness(probe_interval=0.05)
    harness.registry.set_fault("users-v1-1", "down")
    for _ in range(3):
        harness.handle("GET", "/users/1")
    _time.sleep(0.06)
    harness.registry.set_fault("users-v1-1", None)

    claimed_sets: list[list[str]] = []
    lock = threading.Lock()

    def claim():
        got = harness.health.due_probes()
        with lock:
            claimed_sets.append(got)
        for instance_id in got:
            harness.health.probe(instance_id)

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda _: claim(), range(8)))
    claims = [c for c in claimed_sets if c]
    log_case(
        "8 concurrent probes: only one claim wins",
        make_request("GET", "/users/1"),
        non_empty_claims=claims,
    )
    assert sum(len(c) for c in claims) == 1


def test_failover_outcome_is_stable_under_concurrent_jitter(harness):
    # First instance fails for half the burst; all requests must end on the
    # healthy second instance with the same observable result.
    harness.registry.set_fault("users-v1-1", "fail")
    finals: list[str] = []
    lock = threading.Lock()

    def one(_):
        result = harness.handle("GET", "/users/42")
        with lock:
            finals.append(result.instance_id)

    # users-read quota is 8/window: stay inside it to isolate failover.
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(one, range(8)))
    log_case(
        "concurrent jitter: every GET converges to the healthy instance",
        make_request("GET", "/users/42"),
        finals=sorted(set(finals)),
        count=len(finals),
    )
    assert set(finals) == {"users-v1-2"}
