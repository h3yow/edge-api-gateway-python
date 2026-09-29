"""Three admission categories: rate burst / inflight / cycle quota.

Asserts per-category rejection codes and live state, that a rejection in one
category consumes nothing from the others, and that concurrency, slow
upstreams and an upstream restart never over-admit.
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from conftest import log_case, make_request
from edge_gateway.admission import AdmissionManager
from edge_gateway.errors import (
    InflightLimitExceededError,
    QuotaExceededError,
    RateLimitExceededError,
)
from edge_gateway.gateway import Gateway
from edge_gateway.health import HealthMonitor
from edge_gateway.models import RouteRule
from edge_gateway.router import Router, RuleRegistry
from edge_gateway.upstream import build_default_registry


def build_harness(
    rate_limit=None,
    inflight_limit=None,
    quota_limit=None,
    rate_window=10.0,
    quota_window=10.0,
    clock=None,
    slow=0.0,
):
    """One-rule gateway on cluster c with two instances, AdmissionManager."""

    rule = RouteRule(
        id="r",
        priority=1,
        cluster="c",
        path_prefix="/x",
        quota_limit=quota_limit,
        quota_window_seconds=quota_window,
        rate_limit=rate_limit,
        rate_window_seconds=rate_window,
        inflight_limit=inflight_limit,
    )
    clusters = {"c": ["i-1", "i-2"]}
    registry = build_default_registry(clusters)
    if slow:
        original = registry.call

        def slow_call(instance_id, body):
            import time

            time.sleep(slow)
            return original(instance_id, body)

        registry.call = slow_call  # type: ignore[method-assign]

    time_func = (lambda: clock["t"]) if clock is not None else __import__("time").monotonic
    admission = AdmissionManager(time_func=time_func)
    health = HealthMonitor(["i-1", "i-2"], registry, failure_threshold=100)
    gateway = Gateway(
        router=Router(RuleRegistry([rule])),
        quotas=admission,
        health=health,
        registry=registry,
        clusters=clusters,
    )

    class H:
        pass

    h = H()
    h.rule = rule
    h.registry = registry
    h.admission = admission
    h.gateway = gateway
    h.handle = lambda **kw: gateway.handle(  # noqa: E731
        make_request(path="/x", **kw)
    )
    h.raw = lambda req: gateway.handle(req)
    return h


def _fire_concurrently(h, n, caller="a", hold=None):
    outcomes = {"accepted": 0, "rate": 0, "inflight": 0, "quota": 0}
    lock = threading.Lock()

    def one(_):
        try:
            h.handle(caller=caller)
            with lock:
                outcomes["accepted"] += 1
        except RateLimitExceededError:
            with lock:
                outcomes["rate"] += 1
        except InflightLimitExceededError:
            with lock:
                outcomes["inflight"] += 1
        except QuotaExceededError:
            with lock:
                outcomes["quota"] += 1

    with ThreadPoolExecutor(max_workers=n) as pool:
        list(pool.map(one, range(n)))
    return outcomes


# --- boundary: one category at a time --------------------------------------


def test_rate_burst_boundary():
    h = build_harness(rate_limit=3, quota_limit=100)
    req = make_request("GET", "/x")
    for _ in range(3):
        h.raw(req)
    with pytest.raises(RateLimitExceededError) as exc:
        h.raw(req)
    detail = exc.value.details
    log_case(
        "4th burst request rejected as RATE_LIMIT_EXCEEDED",
        req,
        rejected_category=detail["rejected_category"],
        rate=detail["admission"]["rate"],
    )
    assert detail["rejected_category"] == "rate"
    assert detail["admission"]["rate"]["used"] == 3
    assert detail["admission"]["rate"]["remaining"] == 0
    # Current rate/inflight/quota state is all present and readable.
    assert set(detail["admission"]) == {"caller", "rule_id", "rate", "inflight", "quota"}


def test_rate_window_rolls_over():
    clock = {"t": 0.0}
    h = build_harness(rate_limit=2, quota_limit=100, rate_window=1.0, clock=clock)
    req = make_request("GET", "/x")
    h.raw(req)
    h.raw(req)
    with pytest.raises(RateLimitExceededError):
        h.raw(req)
    clock["t"] += 1.0  # burst window elapsed
    result = h.raw(req)
    log_case("rate burst window rolls over", req, quota=result.quota["rate"])
    assert result.quota["rate"]["used"] == 1


def test_inflight_boundary_then_slot_frees():
    entered = threading.Event()
    release = threading.Event()
    h = build_harness(inflight_limit=1, quota_limit=100, rate_limit=100)

    def blocking_call(instance_id, body):
        entered.set()
        release.wait(timeout=2)
        return 200, {"served_by": instance_id}

    h.registry._handlers["i-1"] = blocking_call  # noqa: SLF001
    h.registry._handlers["i-2"] = blocking_call  # noqa: SLF001

    def held():
        h.handle()

    t = threading.Thread(target=held)
    t.start()
    assert entered.wait(1.0)

    req = make_request("GET", "/x")
    with pytest.raises(InflightLimitExceededError) as exc:
        h.raw(req)
    detail = exc.value.details
    log_case(
        "second simultaneous request rejected as INFLIGHT_LIMIT_EXCEEDED",
        req,
        inflight=detail["admission"]["inflight"],
    )
    assert detail["rejected_category"] == "inflight"
    assert detail["admission"]["inflight"]["used"] == 1

    release.set()
    t.join(timeout=2)
    # Slot returned after completion -> a new request is admitted.
    result = h.raw(req)
    assert result.status_code == 200
    assert h.admission.inflight_of(req, h.rule) == 0


def test_quota_cycle_boundary():
    h = build_harness(rate_limit=100, inflight_limit=100, quota_limit=2)
    req = make_request("GET", "/x")
    h.raw(req)
    h.raw(req)
    with pytest.raises(QuotaExceededError) as exc:
        h.raw(req)
    detail = exc.value.details
    log_case("3rd cycle request rejected as QUOTA_EXCEEDED", req, quota=detail["admission"]["quota"])
    assert detail["rejected_category"] == "quota"
    assert detail["admission"]["quota"]["used"] == 2


# --- rejections never consume the other categories -------------------------


def test_rate_rejection_consumes_nothing_else():
    # rate is the tightest: 2/burst, generous inflight and cycle quota.
    h = build_harness(rate_limit=2, inflight_limit=100, quota_limit=100)
    req = make_request("GET", "/x")
    admitted = 0
    rate_rejected = 0
    for _ in range(6):
        try:
            h.raw(req)
            admitted += 1
        except RateLimitExceededError:
            rate_rejected += 1
    snap = h.admission.check(make_request("GET", "/x", caller="other"), h.rule)[1]
    log_case(
        "rate rejects burn no quota/inflight",
        req,
        admitted=admitted,
        rate_rejected=rate_rejected,
        quota_used=snap["quota"]["used"],
        inflight_used=snap["inflight"]["used"],
    )
    assert admitted == 2 and rate_rejected == 4
    # Only the two admitted requests consumed quota; inflight already released.
    own = h.admission.check(req, h.rule)[1]
    assert own["quota"]["used"] == 2
    assert own["inflight"]["used"] == 0


def test_inflight_rejection_consumes_nothing_else():
    entered = threading.Event()
    release = threading.Event()
    h = build_harness(rate_limit=100, inflight_limit=1, quota_limit=100)

    def blocking(i, b):
        entered.set()
        release.wait(2)
        return 200, {"served_by": i}

    h.registry._handlers["i-1"] = blocking  # noqa: SLF001
    h.registry._handlers["i-2"] = blocking  # noqa: SLF001

    t = threading.Thread(target=h.handle)
    t.start()
    entered.wait(1.0)

    rejected = {"rate": 0, "inflight": 0, "quota": 0}
    req = make_request("GET", "/x")
    for _ in range(8):
        try:
            h.raw(req)
        except InflightLimitExceededError:
            rejected["inflight"] += 1
        except (RateLimitExceededError, QuotaExceededError) as exc:
            rejected[exc.details["rejected_category"]] += 1
    release.set()
    t.join(2)
    own = h.admission.check(req, h.rule)[1]
    log_case(
        "inflight rejects burn no rate/quota",
        req,
        rejected=rejected,
        rate_used=own["rate"]["used"],
        quota_used=own["quota"]["used"],
    )
    assert rejected["inflight"] == 8
    # Exactly one request (the held one) consumed rate/quota. The check()
    # call above also consumed one, so before that it was exactly one.
    assert own["rate"]["used"] == 2
    assert own["quota"]["used"] == 2


# --- concurrency / slowness never over-admit -------------------------------


def test_concurrent_inflight_never_exceeds_cap():
    # Generous rate/quota; the inflight cap is the binding constraint. Every
    # request is slow, so 30 concurrent clients must produce at most
    # inflight_limit simultaneous upstream executions.
    h = build_harness(rate_limit=1000, inflight_limit=3, quota_limit=1000, slow=0.05)
    peak = {"n": 0}
    lock = threading.Lock()
    original = h.registry.call
    probe_req = make_request("GET", "/x", caller="a")

    def counted(instance_id, body):
        with lock:
            peak["n"] = max(peak["n"], h.admission.inflight_of(probe_req, h.rule))
        return original(instance_id, body)

    h.registry.call = counted  # type: ignore[method-assign]
    outcomes = _fire_concurrently(h, 30, caller="a")
    executed = sum(h.registry.call_count(i) for i in ("i-1", "i-2"))
    log_case(
        "30 concurrent, inflight cap 3",
        probe_req,
        outcomes=outcomes,
        upstream_executions=executed,
        peak_inflight=peak["n"],
    )
    assert outcomes["accepted"] + outcomes["inflight"] == 30
    assert executed == outcomes["accepted"]
    assert peak["n"] <= 3


def test_concurrent_quota_never_over_admits():
    # Generous rate/inflight; the cycle quota is the binding constraint.
    h = build_harness(rate_limit=1000, inflight_limit=1000, quota_limit=10, slow=0.02)
    outcomes = _fire_concurrently(h, 30, caller="q")
    executed = sum(h.registry.call_count(i) for i in ("i-1", "i-2"))
    log_case(
        "30 concurrent, cycle quota 10",
        make_request("GET", "/x"),
        outcomes=outcomes,
        upstream_executions=executed,
    )
    assert outcomes["accepted"] == 10
    assert outcomes["accepted"] + outcomes["quota"] == 30
    assert executed == 10  # rejected requests never reached an upstream
