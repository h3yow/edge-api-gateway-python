"""Per-caller quotas: exact boundaries, concurrency, slow upstreams, windows."""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from conftest import Harness, log_case, make_request
from edge_gateway.admission import AdmissionManager
from edge_gateway.errors import QuotaExceededError
from edge_gateway.gateway import Gateway

ORDERS_DEFAULT_LIMIT = 4


def _fire(harness, n, method="GET", path="/orders/1", caller="alice"):
    outcomes = {"accepted": 0, "rejected": 0}
    lock = threading.Lock()

    def one():
        try:
            harness.handle(method, path, caller=caller)
            with lock:
                outcomes["accepted"] += 1
        except QuotaExceededError:
            with lock:
                outcomes["rejected"] += 1

    with ThreadPoolExecutor(max_workers=n) as pool:
        list(pool.map(lambda _: one(), range(n)))
    return outcomes


def test_exact_quota_boundary_sequential(harness):
    req = make_request("GET", "/orders/1", caller="alice")
    accepted, rejected = 0, 0
    first_reject_details = None
    for _ in range(ORDERS_DEFAULT_LIMIT + 3):
        try:
            harness.gateway.handle(req)
            accepted += 1
        except QuotaExceededError as exc:
            rejected += 1
            first_reject_details = exc.details
    log_case(
        "fixed window admits exactly the limit",
        req,
        accepted=accepted,
        rejected=rejected,
        first_reject=first_reject_details,
    )
    assert accepted == ORDERS_DEFAULT_LIMIT
    assert rejected == 3
    assert first_reject_details["quota"]["used"] == ORDERS_DEFAULT_LIMIT


def test_concurrent_requests_never_exceed_quota(harness):
    n = 40
    outcomes = _fire(harness, n)
    log_case(
        "40 concurrent callers, limit 4: never over-admit",
        make_request("GET", "/orders/1", caller="alice"),
        outcomes=outcomes,
    )
    assert outcomes["accepted"] == ORDERS_DEFAULT_LIMIT
    assert outcomes["accepted"] + outcomes["rejected"] == n


def test_slow_upstream_still_enforces_quota(harness, monkeypatch):
    # Make every successful upstream call slow; quota is consumed BEFORE the
    # upstream is touched, so slow calls cannot let extras through.
    original_call = harness.registry.call
    barrier = threading.Barrier(ORDERS_DEFAULT_LIMIT)

    def slow_call(instance_id, body):
        try:
            barrier.wait(timeout=2)
        except threading.BrokenBarrierError:
            pass
        time.sleep(0.05)
        return original_call(instance_id, body)

    harness.registry.call = slow_call  # type: ignore[method-assign]
    n = 20
    outcomes = _fire(harness, n)
    upstream_calls = sum(harness.registry.call_count(i) for i in harness.health.snapshot())
    log_case(
        "slow upstreams cannot widen the quota window",
        make_request("GET", "/orders/1"),
        outcomes=outcomes,
        upstream_user_request_executions=upstream_calls,
    )
    assert outcomes["accepted"] == ORDERS_DEFAULT_LIMIT
    assert upstream_calls == ORDERS_DEFAULT_LIMIT


def test_rejected_requests_consume_no_upstream_resource(harness):
    req = make_request("GET", "/orders/1", caller="alice")
    for _ in range(ORDERS_DEFAULT_LIMIT):
        harness.gateway.handle(req)
    snapshot_before = {
        i: harness.registry.call_count(i) for i in harness.health.snapshot()
    }
    for _ in range(10):
        with pytest.raises(QuotaExceededError):
            harness.gateway.handle(req)
    snapshot_after = {i: harness.registry.call_count(i) for i in harness.health.snapshot()}
    log_case(
        "rejected calls never reach an upstream",
        req,
        executions_before=snapshot_before,
        executions_after=snapshot_after,
    )
    assert snapshot_before == snapshot_after


def test_quotas_are_isolated_per_caller(harness):
    alice_rejected = bob_rejected = False
    for _ in range(ORDERS_DEFAULT_LIMIT + 1):
        try:
            harness.handle("GET", "/orders/1", caller="alice")
        except QuotaExceededError:
            alice_rejected = True
        try:
            harness.handle("GET", "/orders/1", caller="bob")
        except QuotaExceededError:
            bob_rejected = True
    # Both callers got exactly the limit, independently.
    alice_accepted = harness.registry.call_count("orders-v1-1") + harness.registry.call_count(
        "orders-v1-2"
    )
    log_case(
        "caller isolation: separate counters per caller",
        make_request("GET", "/orders/1", caller="bob"),
        alice_rejected=alice_rejected,
        bob_rejected=bob_rejected,
        total_orders_executions=alice_accepted,
    )
    assert alice_rejected and bob_rejected
    assert alice_accepted == 2 * ORDERS_DEFAULT_LIMIT


def test_window_rolls_over_and_quota_refreshes():
    clock = {"t": 0.0}
    quotas = AdmissionManager(time_func=lambda: clock["t"])
    harness = Harness()
    harness.quotas = quotas
    harness.gateway = Gateway(
        router=harness.gateway._router,
        quotas=quotas,
        health=harness.health,
        registry=harness.registry,
        clusters=harness.clusters,
    )
    req = make_request("GET", "/orders/1", caller="alice")
    for _ in range(ORDERS_DEFAULT_LIMIT):
        harness.gateway.handle(req)
    with pytest.raises(QuotaExceededError):
        harness.gateway.handle(req)
    clock["t"] += 1.0  # next window
    refreshed = harness.gateway.handle(req)
    log_case(
        "new fixed window resets the counter",
        req,
        refreshed_rule=refreshed.rule_id,
        quota_snapshot=refreshed.quota,
    )
    assert refreshed.quota["used"] == 1
