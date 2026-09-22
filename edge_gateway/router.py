"""Deterministic rule matching.

Matching semantics
------------------
* Every condition present on a rule must match (AND): HTTP path prefix,
  method, exact header values, exact query values, caller id.
* Among all matching rules the one with the highest ``priority`` wins.
* If two or more matching rules share the highest priority, that is a
  conflict: the gateway returns ROUTE_CONFLICT and names the conflicting
  rules instead of silently picking one.
* If no rule matches, the gateway returns NO_ROUTE.

Rule identity (the rule id) breaks ordering ties *only for display*; it is
never used to silently resolve a real conflict.
"""

from __future__ import annotations

from edge_gateway.errors import NoRouteError, RouteConflictError
from edge_gateway.models import RequestContext, RouteRule


class RuleRegistry:
    """Validates and holds the configured rule set."""

    def __init__(self, rules: list[RouteRule]) -> None:
        ids: set[str] = set()
        for rule in rules:
            if rule.id in ids:
                raise ValueError(f"duplicate rule id: {rule.id!r}")
            ids.add(rule.id)
        self._rules = list(rules)

    @property
    def rules(self) -> list[RouteRule]:
        return list(self._rules)


def _rule_matches(rule: RouteRule, request: RequestContext) -> tuple[bool, list[str]]:
    """Evaluate one rule, returning (matches, human-readable reasons)."""

    reasons: list[str] = []
    ok = True

    if rule.path_prefix is not None:
        hit = request.path == rule.path_prefix or request.path.startswith(
            rule.path_prefix.rstrip("/") + "/"
        )
        reasons.append(f"path {request.path!r} prefix {rule.path_prefix!r} -> {hit}")
        ok = ok and hit

    if rule.methods is not None:
        hit = request.method in rule.methods
        reasons.append(f"method {request.method!r} in {sorted(rule.methods)} -> {hit}")
        ok = ok and hit

    for name, expected in sorted(rule.header_equals.items()):
        actual = request.headers.get(name.lower())
        hit = actual == expected
        reasons.append(f"header {name!r}={actual!r} expected {expected!r} -> {hit}")
        ok = ok and hit

    for name, expected in sorted(rule.query_equals.items()):
        actual = request.query.get(name)
        hit = actual == expected
        reasons.append(f"query {name!r}={actual!r} expected {expected!r} -> {hit}")
        ok = ok and hit

    if rule.callers is not None:
        hit = request.caller in rule.callers
        reasons.append(f"caller {request.caller!r} in {sorted(rule.callers)} -> {hit}")
        ok = ok and hit

    return ok, reasons


class Router:
    def __init__(self, registry: RuleRegistry) -> None:
        self._registry = registry

    def explain(self, request: RequestContext) -> dict:
        """Evaluate every rule and return the full decision basis.

        The result contains each rule's individual conditions, the set of
        matching rules, and how the final selection was made. This structure
        powers the ``/__explain`` endpoint and the test logs.
        """

        evaluations: list[dict] = []
        matched: list[RouteRule] = []
        for rule in self._registry.rules:
            ok, reasons = _rule_matches(rule, request)
            evaluations.append(
                {"rule_id": rule.id, "priority": rule.priority, "matched": ok, "reasons": reasons}
            )
            if ok:
                matched.append(rule)

        matched.sort(key=lambda r: (-r.priority, r.id))
        decision = "no_match"
        selected: RouteRule | None = None
        conflict_with: list[str] = []

        if matched:
            top_priority = matched[0].priority
            winners = [r for r in matched if r.priority == top_priority]
            if len(winners) > 1:
                decision = "conflict"
                conflict_with = sorted(r.id for r in winners)
            else:
                decision = "selected"
                selected = winners[0]

        return {
            "request": {
                "method": request.method,
                "path": request.path,
                "caller": request.caller,
                "headers": dict(request.headers),
                "query": dict(request.query),
            },
            "evaluations": evaluations,
            "matched_rule_ids": [r.id for r in matched],
            "decision": decision,
            "selected_rule_id": selected.id if selected else None,
            "conflicting_rule_ids": conflict_with,
        }

    def route(self, request: RequestContext) -> RouteRule:
        result = self.explain(request)
        if result["decision"] == "conflict":
            raise RouteConflictError(
                "multiple rules share the highest matching priority",
                details={
                    "conflicting_rule_ids": result["conflicting_rule_ids"],
                    "matched_rule_ids": result["matched_rule_ids"],
                },
            )
        if result["decision"] == "no_match":
            raise NoRouteError(
                "no routing rule matched the request",
                details={"matched_rule_ids": []},
            )
        rule_id = result["selected_rule_id"]
        return next(r for r in self._registry.rules if r.id == rule_id)
