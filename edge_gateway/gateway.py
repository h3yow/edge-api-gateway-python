"""Gateway request pipeline.

Order of decisions (each step records trace lines)::

    1. normalize request (done by the ASGI layer)
    2. route        -> rule, or NO_ROUTE / ROUTE_CONFLICT
    3. quota        -> accept-and-consume, or QUOTA_EXCEEDED (no upstream call)
    4. pick healthy instances deterministically (cluster order)
    5. dispatch with bounded failover:
         - UpstreamUnreachable: connection failed before execution;
           every method may try the next instance.
         - HTTP 500 / UpstreamTimeout: execution happened or is unconfirmed;
           only idempotent methods (GET/HEAD/OPTIONS) may be retried, a
           non-idempotent request fails fast as NON_IDEMPOTENT_NOT_RETRIED
           so it is never executed twice.
    6. feed outcomes back to the HealthMonitor (trip / recover).

Probes of due instances are opportunistically executed while selecting a
target, under the HealthMonitor's atomic claim, so concurrent requests
cannot probe the same instance twice.
"""

from __future__ import annotations

import threading
from typing import Callable

from edge_gateway.errors import (
    AllUpstreamsUnavailableError,
    GatewayError,
    InflightLimitExceededError,
    NonIdempotentRetryError,
    QuotaExceededError,
    RateLimitedError,
)
from edge_gateway.health import HealthMonitor
from edge_gateway.models import AttemptRecord, ForwardResult, RequestContext
from edge_gateway.admission import AdmissionManager, INFLIGHT, QUOTA, RATE
from edge_gateway.realupstream import HttpUpstreamRegistry, UpstreamResponse
from edge_gateway.router import Router
from edge_gateway.upstream import (
    UpstreamRegistry,
    UpstreamTimeout,
    UpstreamUnreachable,
)

IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


class Gateway:
    def __init__(
        self,
        router: Router,
        quotas,
        health: HealthMonitor,
        registry,
        clusters: dict[str, list[str]],
        max_attempts: int = 3,
        time_func: Callable[[], float] = __import__("time").monotonic,
        transparent: bool | None = None,
        recovery=None,
    ) -> None:
        self._router = router
        # Accepts either the new three-category AdmissionManager or the
        # legacy QuotaManager (which exposes the same check/reset surface).
        self._quotas = quotas
        self._admission = isinstance(quotas, AdmissionManager)
        self._health = health
        self._registry = registry
        self._clusters = clusters
        self._max_attempts = max_attempts
        self._time = time_func
        # Real network upstreams are forwarded byte-for-byte by default;
        # simulated ones keep the explainable JSON envelope.
        if transparent is None:
            transparent = isinstance(registry, HttpUpstreamRegistry)
        self._transparent = transparent
        self._recovery = recovery

    # -- explanation -------------------------------------------------------

    def explain(self, request: RequestContext) -> dict:
        decision = self._router.explain(request)
        out = {"routing": decision}
        rule_id = decision.get("selected_rule_id")
        if rule_id is not None:
            rule = next(r for r in self._router._registry.rules if r.id == rule_id)  # noqa: SLF001
            instances = self._clusters[rule.cluster]
            out["cluster"] = rule.cluster
            out["instances"] = instances
            out["health"] = {i: self._health.snapshot()[i] for i in instances}
            out["quota"] = {
                "limit": rule.quota_limit,
                "window_seconds": rule.quota_window_seconds,
                "rate_limit": rule.rate_limit,
                "rate_window_seconds": rule.rate_window_seconds,
                "inflight_limit": rule.inflight_limit,
            }
        return out

    # -- main pipeline -----------------------------------------------------

    def handle(self, request: RequestContext) -> ForwardResult:
        result = ForwardResult()
        trace = result.trace
        trace.append(
            f"request: {request.method} {request.path} caller={request.caller!r}"
        )

        # 1. routing (may raise NO_ROUTE / ROUTE_CONFLICT)
        rule = self._router.route(request)
        result.rule_id = rule.id
        result.cluster = rule.cluster
        trace.append(f"route: selected rule {rule.id!r} -> cluster {rule.cluster!r}")

        # 2. admission (atomic rate+inflight+quota; rejection touches no
        #    upstream and consumes nothing in any category)
        allowed, admission_snapshot, admission_trace = self._quotas.check(request, rule)
        trace.extend(admission_trace)
        result.admission = admission_snapshot
        # Backward-compatible single-category view.
        result.quota = admission_snapshot.get(QUOTA, admission_snapshot)
        if not allowed:
            raise _admission_error(rule, request, admission_snapshot, trace)

        # 3. deterministic instance order, filtered by health. The inflight
        #    slot is released on every exit path (success, upstream error or
        #    no-available-instance rejection).
        ordered = list(self._clusters[rule.cluster])
        try:
            return self._dispatch(request, rule, ordered, result)
        finally:
            if self._admission:
                self._quotas.release(request, rule)

    def _select_targets(self, cluster_instances: list[str]) -> tuple[list[str], list[str]]:
        # Opportunistically run any due recovery probes first. Claiming is
        # atomic inside HealthMonitor, so this is safe under concurrency.
        probe_trace: list[str] = []
        for instance_id in self._health.due_probes():
            reopened = self._health.probe(instance_id)
            probe_trace.append(
                f"probe: {instance_id} recovered and back in rotation"
                if reopened
                else f"probe: {instance_id} still unavailable"
            )
        return self._health.available_in(cluster_instances), probe_trace

    def _call_upstream(self, instance_id: str, request: RequestContext):
        """Dispatch via either the network or the in-process registry."""

        if isinstance(self._registry, HttpUpstreamRegistry):
            return self._registry.call(instance_id, request)
        # In-process simulated registry keeps its (instance_id, body) shape.
        status, payload = self._registry.call(instance_id, request.body)
        return UpstreamResponse(status=status, body=_json_bytes(payload), headers={})

    def _dispatch(
        self,
        request: RequestContext,
        rule,
        ordered: list[str],
        result: ForwardResult,
    ) -> ForwardResult:
        trace = result.trace

        targets, probe_trace = self._select_targets(ordered)
        trace.extend(probe_trace)
        if not targets:
            trace.append(
                f"dispatch: cluster {rule.cluster!r} has no available instance"
            )
            raise AllUpstreamsUnavailableError(
                "every instance in the cluster is unavailable",
                details={
                    "rule_id": rule.id,
                    "cluster": rule.cluster,
                    "instances": ordered,
                },
            )
        trace.append(f"dispatch: healthy order = {targets}")

        idempotent = request.method in IDEMPOTENT_METHODS
        attempts = 0
        last_error: str | None = None

        for instance_id in targets:
            if attempts >= self._max_attempts:
                trace.append(
                    f"dispatch: attempt budget {self._max_attempts} exhausted"
                )
                break
            attempts += 1
            retried = attempts > 1
            try:
                response = self._call_upstream(instance_id, request)
                status = response.status
            except UpstreamUnreachable as exc:
                # Handler never ran: failover is always safe.
                last_error = str(exc)
                result.attempts.append(
                    AttemptRecord(instance_id, "unreachable", str(exc), retried)
                )
                trace.extend(
                    self._health.record_failure(instance_id, reason=f"unreachable: {exc}")
                )
                trace.append(
                    f"dispatch: {instance_id} unreachable before execution ->"
                    " failover is safe"
                )
                continue
            except UpstreamTimeout as exc:
                # Execution is unconfirmed.
                last_error = str(exc)
                result.attempts.append(
                    AttemptRecord(instance_id, "timeout", str(exc), retried)
                )
                trace.extend(
                    self._health.record_failure(instance_id, reason=f"timeout: {exc}")
                )
                if not idempotent:
                    trace.append(
                        f"dispatch: {instance_id} timed out and request is"
                        " non-idempotent -> NOT retried (avoid double execution)"
                    )
                    raise NonIdempotentRetryError(
                        "upstream timed out on a non-idempotent request;"
                        " failover suppressed to avoid double execution",
                        details={
                            "rule_id": rule.id,
                            "instance_id": instance_id,
                            "attempts": [a.__dict__ for a in result.attempts],
                        },
                    )
                trace.append(
                    f"dispatch: {instance_id} timeout, idempotent -> try next"
                )
                continue

            # Handler definitely executed.
            if 500 <= status < 600:
                result.attempts.append(
                    AttemptRecord(instance_id, "http_5xx", str(status), retried)
                )
                trace.extend(
                    self._health.record_failure(
                        instance_id, reason=f"http_{status}"
                    )
                )
                last_error = f"HTTP {status}"
                if not idempotent:
                    trace.append(
                        f"dispatch: {instance_id} returned {status} and request is"
                        " non-idempotent -> NOT retried (already executed once)"
                    )
                    raise NonIdempotentRetryError(
                        "upstream executed the non-idempotent request and failed;"
                        " failover suppressed to avoid double execution",
                        details={
                            "rule_id": rule.id,
                            "instance_id": instance_id,
                            "upstream_status": status,
                            "attempts": [a.__dict__ for a in result.attempts],
                        },
                    )
                trace.append(
                    f"dispatch: {instance_id} HTTP {status}, idempotent -> try next"
                )
                continue

            # Success (including 4xx, which is an application answer).
            self._health.record_success(instance_id)
            result.attempts.append(
                AttemptRecord(instance_id, "success", f"HTTP {status}", retried)
            )
            result.status_code = status if self._transparent else 200
            result.instance_id = instance_id
            result.upstream_status = status
            if self._transparent:
                # Real network mode: status, body and headers go back to the
                # caller verbatim.
                result.raw_body = response.body
                result.raw_headers = dict(response.headers)
            else:
                result.upstream_body = _json_loads(response.body)
            trace.append(f"dispatch: {instance_id} answered HTTP {status} -> done")
            return result

        trace.append(
            f"dispatch: no instance could serve the request; last_error={last_error}"
        )
        raise AllUpstreamsUnavailableError(
            "all attempted upstreams failed",
            details={
                "rule_id": rule.id,
                "cluster": rule.cluster,
                "instances": ordered,
                "last_error": last_error,
                "attempts": [a.__dict__ for a in result.attempts],
            },
        )


def _json_bytes(payload: dict) -> bytes:
    import json

    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _json_loads(raw: bytes):
    import json

    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {"raw": raw.decode("utf-8", errors="replace")}


_ADMISSION_ERRORS = {
    RATE: (
        RateLimitedError,
        "caller is sending too fast: short-burst rate limit reached",
    ),
    INFLIGHT: (
        InflightLimitExceededError,
        "caller has too many requests already in flight",
    ),
    QUOTA: (
        QuotaExceededError,
        "caller quota exhausted for this rule in the current window",
    ),
}


def _admission_error(rule, request, snapshot, trace):
    category = snapshot.get("rejected_category", QUOTA)
    error_cls, message = _ADMISSION_ERRORS[category]
    trace.append("admission: request rejected before touching any upstream")
    return error_cls(
        message,
        details={
            "rule_id": rule.id,
            "cluster": rule.cluster,
            "caller": request.caller,
            "rejected_category": category,
            "admission": snapshot,
            # Flat per-category views for direct inspection.
            "rate": snapshot.get(RATE),
            "inflight": snapshot.get(INFLIGHT),
            "quota": snapshot.get(QUOTA),
        },
    )
