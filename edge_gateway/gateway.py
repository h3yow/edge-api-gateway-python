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
    NonIdempotentRetryError,
)
from edge_gateway.health import HealthMonitor
from edge_gateway.models import AttemptRecord, ForwardResult, RequestContext
from edge_gateway.quota import QuotaManager
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
        quotas: QuotaManager,
        health: HealthMonitor,
        registry: UpstreamRegistry,
        clusters: dict[str, list[str]],
        max_attempts: int = 3,
        time_func: Callable[[], float] = __import__("time").monotonic,
    ) -> None:
        self._router = router
        self._quotas = quotas
        self._health = health
        self._registry = registry
        self._clusters = clusters
        self._max_attempts = max_attempts
        self._time = time_func

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

        # 2. quota (atomic consume; rejection touches no upstream resource)
        allowed, quota_snapshot, quota_trace = self._quotas.check(request, rule)
        trace.extend(quota_trace)
        result.quota = quota_snapshot
        if not allowed:
            raise _quota_error(rule, request, quota_snapshot, trace)

        # 3. deterministic instance order, filtered by health
        ordered = list(self._clusters[rule.cluster])
        return self._dispatch(request, rule, ordered, result)

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
                status, body = self._registry.call(instance_id, request.body)
            except UpstreamUnreachable as exc:
                # Handler never ran: failover is always safe.
                last_error = str(exc)
                result.attempts.append(
                    AttemptRecord(instance_id, "unreachable", str(exc), retried)
                )
                trace.extend(
                    self._health.record_failure(instance_id)
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
                trace.extend(self._health.record_failure(instance_id))
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
                trace.extend(self._health.record_failure(instance_id))
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
            result.status_code = 200
            result.instance_id = instance_id
            result.upstream_status = status
            result.upstream_body = body
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


def _quota_error(rule, request, snapshot, trace):
    from edge_gateway.errors import QuotaExceededError

    trace.append("quota: request rejected before touching any upstream")
    return QuotaExceededError(
        "caller quota exhausted for this rule in the current window",
        details={
            "rule_id": rule.id,
            "cluster": rule.cluster,
            "caller": request.caller,
            "quota": snapshot,
        },
    )
