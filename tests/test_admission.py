"""Fine-grained admission: rate / inflight / quota boundaries and concurrency.

The demo rule ``payments-api`` (path ``/payments``) carries all three limits
at once:

    rate_limit=3 / 1s, inflight_limit=2, quota_limit=5 / 1s
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from conftest import Harness, log_case, make_request
from edge_gateway.admission import AdmissionManager
from edge_gateway.errors import (
    InflightLimitExceededError,
    QuotaExceededError,
    RateLimitedError,
)
from edge_gateway.gateway import Gateway

PATH = "/payments/1"
RATE = 3
INFLIGHT = 2
QUOTA = 5


def _fire(harness, n, method="GET", path=PATH, caller="acme"):
    outcomes = {"allowed": 0, "rate": 0, "inflight": 0, "quota": 0}
    lock = threading.Lock()

    def one():
        try:
            harness.handle(method, path, caller=caller)
            with lock:
                outcomes["allowed"] += 1
        except RateLimitedError:
            with lock:
                outcomes["rate"] += 1
        except InflightLimitExceededError:
            with lock:
                outcomes["inflight"] += 1
        except QuotaExceededError:
            with lock:
                outcomes["quota"] += 1

    with ThreadPoolExecutor(max_workers=n) as pool:
        list(pool.map(lambda _: one(), range(n)))
    return outcomes


def test_rule_carries_three_independent_limits(harness):
    rule = next(r for r in harness.rules if r.id == "payments-api")
    log_case("payments-api policy", policy=rule.admission_policy())
    assert rule.rate_limit == RATE
    assert rule.inflight_limit == INFLIGHT
    assert rule.quota_limit == QUOTA


def test_rate_limit_boundary_is_first_to_reject(harness):
    # Within one burst window the 4th request is rejected by RATE before
    # either inflight or quota can bind.
    req = make_request("GET", PATH, caller="r1")
    for i in range(RATE):
        harness.gateway.handle(req)
    with pytest.raises(RateLimitedError) as exc:
        harness.gateway.handle(req)
    snap = exc.value.details["admission"]
    log_case(
        "burst #4 -> RATE_LIMITED with full state",
        req,
        category=snap["rejected_category"],
        rate=snap["rate"],
        inflight=snap["inflight"],
        quota=snap["quota"],
    )
    assert exc.value.code == "RATE_LIMITED"
    assert snap["rejected_category"] == "rate"
    assert snap["rate"]["used"] == RATE
    assert snap["rate"]["remaining"] == 0
    # The other categories are explained even though rate fired.
    assert snap["inflight"]["limit"] == INFLIGHT
    assert snap["quota"]["limit"] == QUOTA


def test_quota_boundary_after_window(harness):
    clock = {"t": 0.0}
    admission = AdmissionManager(time_func=lambda: clock["t"])
    harness.quotas = admission
    harness.gateway = Gateway(
        router=harness.gateway._router,
        quotas=admission,
        health=harness.health,
        registry=harness.registry,
        clusters=harness.clusters,
    )
    req = make_request("GET", PATH, caller="q1")
    # Rate window is 1s, quota window is 3s. Drain the 5 quota units at a
    # pace that never trips the rate limit (at most 2 per burst window):
    #   t=0: 2 allowed -> quota used 2, then advance to t=1
    #   t=1: 2 allowed -> quota used 4, then advance to t=2
    #   t=2: 1 allowed  -> quota used 5, then advance to t=3 then 2.5
    for allowed_at in (2, 2, 1):
        for _ in range(allowed_at):
            harness.gateway.handle(req)
        clock["t"] += 1.0
    # At t=2.5 the first quota window (0..3s) is still open, and the burst
    # window (2..3s) already has 1 request, so one more is rate-allowed but
    # quota-exhausted.
    clock["t"] = 2.5
    with pytest.raises(QuotaExceededError) as exc:
        harness.gateway.handle(req)
    snap = exc.value.details["admission"]
    log_case(
        "6th unit in quota window -> QUOTA_EXCEEDED",
        req,
        quota=snap["quota"],
        rate=snap["rate"],
    )
    assert snap["rejected_category"] == "quota"
    assert snap["quota"]["used"] == QUOTA
    assert snap["quota"]["remaining"] == 0
    # After the *quota* window (3s) rolls over, budget returns.
    clock["t"] = 3.5
    refreshed = harness.gateway.handle(req)
    assert refreshed.quota["used"] == 1


def test_inflight_boundary_blocks_when_requests_are_still_executing(harness):
    entered = threading.Event()
    release = threading.Event()

    def slow_call(instance_id, body):
        entered.set()
        release.wait(timeout=2)
        return 200, {"served_by": instance_id, "body_bytes": len(body)}

    harness.registry.call = slow_call  # type: ignore[method-assign]

    req = make_request("GET", PATH, caller="i1")
    worker = threading.Thread(target=harness.gateway.handle, args=(req,))
    worker.start()
    assert entered.wait(timeout=2)
    # One request is in flight; one more fits (limit 2), the third is
    # rejected for INFLIGHT - not rate (used is 2 < 3).
    second_ok = threading.Event()

    def second():
        harness.gateway.handle(req)
        second_ok.set()

    t2 = threading.Thread(target=second)
    t2.start()
    time.sleep(0.1)  # let the second claim its inflight slot
    with pytest.raises(InflightLimitExceededError) as exc:
        harness.gateway.handle(req)
    snap = exc.value.details["admission"]
    log_case(
        "3rd simultaneous request -> INFLIGHT_LIMIT_EXCEEDED",
        req,
        inflight=snap["inflight"],
        rate=snap["rate"],
    )
    assert snap["rejected_category"] == "inflight"
    assert snap["inflight"]["inflight"] == INFLIGHT
    release.set()
    worker.join(2)
    t2.join(2)
    assert second_ok.is_set()
    # Slots are released after completion: another request fits again.
    result = harness.gateway.handle(make_request("GET", PATH, caller="i1"))
    assert result.instance_id


def test_rejected_request_consumes_no_category_or_upstream(harness):
    req = make_request("GET", PATH, caller="r2")
    before = {i: harness.registry.call_count(i) for i in harness.health.snapshot()}
    for _ in range(RATE):
        harness.gateway.handle(req)
    for _ in range(10):
        with pytest.raises(RateLimitedError):
            harness.gateway.handle(req)
    # New caller: inflight rejection must also consume nothing.
    entered = threading.Event()
    release = threading.Event()

    def slow_call(instance_id, body):
        entered.set()
        release.wait(timeout=2)
        return 200, {"served_by": instance_id}

    harness.registry.call = slow_call  # type: ignore[method-assign]
    threads = [
        threading.Thread(target=harness.gateway.handle,
                         args=(make_request("GET", PATH, caller="i2"),))
        for _ in range(INFLIGHT)
    ]
    for t in threads:
        t.start()
    assert entered.wait(timeout=2)
    for _ in range(5):
        with pytest.raises(InflightLimitExceededError):
            harness.gateway.handle(make_request("GET", PATH, caller="i2"))
    release.set()
    for t in threads:
        t.join(2)
    after = {i: harness.registry.call_count(i) for i in harness.health.snapshot()}
    # The slow monkeypatched handler does not touch the registry counter; the
    # only counted executions are the 3 rate-allowed requests above. The key
    # assertion is that the 15 rejected requests added zero executions.
    log_case(
        "rejected calls consume nothing and never reach upstreams",
        req,
        executions_delta={i: after[i] - before.get(i, 0) for i in after},
    )
    assert sum(after.values()) - sum(before.values()) == RATE


def test_concurrent_burst_never_exceeds_rate_limit(harness):
    outcomes = _fire(harness, 50)
    log_case("50 concurrent requests, rate limit 3", outcomes=outcomes)
    assert outcomes["allowed"] == RATE
    assert outcomes["rate"] == 50 - RATE
    assert outcomes["inflight"] == 0
    assert outcomes["quota"] == 0


def test_concurrent_slow_upstream_never_exceeds_inflight_limit(harness):
    # A barrier of INFLIGHT workers means only that many can ever be inside
    # the upstream simultaneously; every other request must be rejected
    # (rate or inflight) before touching the upstream.
    barrier = threading.Barrier(INFLIGHT)

    def slow_call(instance_id, body):
        try:
            barrier.wait(timeout=2)
        except threading.BrokenBarrierError:
            pass
        time.sleep(0.05)
        return 200, {"served_by": instance_id}

    harness.registry.call = slow_call  # type: ignore[method-assign]
    max_seen = {"n": 0}
    seen_lock = threading.Lock()
    current = {"n": 0}

    # Wrap once more to observe concurrency inside the upstream.
    def observing_call(instance_id, body):
        with seen_lock:
            current["n"] += 1
            max_seen["n"] = max(max_seen["n"], current["n"])
        try:
            return slow_call(instance_id, body)
        finally:
            with seen_lock:
                current["n"] -= 1

    harness.registry.call = observing_call  # type: ignore[method-assign]
    outcomes = _fire(harness, 30)
    log_case(
        "slow upstream: concurrent in-flight never above limit",
        outcomes=outcomes,
        max_concurrent_inside_upstream=max_seen["n"],
    )
    assert max_seen["n"] <= INFLIGHT
    assert outcomes["allowed"] <= RATE  # rate also caps within the window


def test_inflight_slot_released_after_upstream_failure():
    # Even when the upstream 500s (or errors) the inflight slot must come
    # back, otherwise one failure would permanently shrink capacity.
    harness = Harness()
    harness.registry.set_fault("users-v1-1", "fail")
    # /payments routes to users-v1; both instances failing forces an error.
    harness.registry.set_fault("users-v1-2", "fail")
    from edge_gateway.errors import NonIdempotentRetryError

    req = make_request("POST", PATH, caller="rel")
    with pytest.raises(NonIdempotentRetryError):
        harness.gateway.handle(req)
    harness.registry.set_fault("users-v1-1", None)
    harness.registry.set_fault("users-v1-2", None)
    # Slot was released: GET works again (subject to fresh rate budget).
    result = harness.gateway.handle(make_request("GET", PATH, caller="rel2"))
    log_case("inflight slot released after failed POST", result=result.instance_id)
    assert result.instance_id
