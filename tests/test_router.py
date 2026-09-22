"""Routing: deterministic priority, conflicts, no-match and explainability."""

from __future__ import annotations

import pytest

from conftest import Harness, log_case, make_request
from edge_gateway.errors import NoRouteError, RouteConflictError


def test_explicit_priority_picks_highest_matching_rule(harness):
    req = make_request("GET", "/users/7", caller="alice", headers={"x-canary": "true"})
    result = harness.gateway.handle(req)
    log_case(
        "canary header wins over normal read rule",
        req,
        basis=harness.gateway.explain(req)["routing"],
        chosen=result.rule_id,
        instance=result.instance_id,
    )
    assert result.rule_id == "users-canary"
    assert result.cluster == "users-v2"


def test_same_input_is_routed_identically_from_any_entry(harness):
    req = make_request("GET", "/orders/9", caller="vip")
    first = harness.gateway.handle(req)
    second = harness.gateway.handle(req)  # entry-independent normalization
    log_case(
        "identical request -> identical decision",
        req,
        first_choice=(first.rule_id, first.instance_id),
        second_choice=(second.rule_id, second.instance_id),
    )
    assert (first.rule_id, first.cluster) == (second.rule_id, second.cluster)
    assert first.rule_id == "orders-vip"


def test_caller_scoped_routing(harness):
    vip = make_request("GET", "/orders/1", caller="vip")
    normal = make_request("GET", "/orders/1", caller="bob")
    r1 = harness.gateway.handle(vip)
    r2 = harness.gateway.handle(normal)
    log_case(
        "caller dimension changes cluster",
        normal,
        vip_choice=r1.rule_id,
        normal_choice=r2.rule_id,
    )
    assert r1.cluster == "orders-v2"
    assert r2.cluster == "orders-v1"


def test_conflicting_equal_priority_rules_are_rejected_not_guessed(harness):
    req = make_request("GET", "/conflict", headers={"x-experiment": "A"})
    explanation = harness.gateway.explain(req)["routing"]
    with pytest.raises(RouteConflictError) as exc:
        harness.gateway.handle(req)
    log_case(
        "equal-priority conflict must error deterministically",
        req,
        basis=explanation,
        error=exc.value.code,
        conflicting=exc.value.details["conflicting_rule_ids"],
    )
    assert exc.value.status_code == 409
    assert set(exc.value.details["conflicting_rule_ids"]) == {
        "conflict-alpha",
        "conflict-beta",
    }


def test_no_matching_rule_is_a_distinguishable_error(harness):
    req = make_request("GET", "/does/not/exist")
    with pytest.raises(NoRouteError) as exc:
        harness.gateway.handle(req)
    log_case(
        "unmatched request is never dropped silently",
        req,
        basis=harness.gateway.explain(req)["routing"],
        error=exc.value.code,
    )
    assert exc.value.status_code == 404
    assert exc.value.code == "NO_ROUTE"


def test_writes_route_to_writer_rule(harness):
    req = make_request("POST", "/users", caller="carol", body=b'{"name":"n"}')
    result = harness.gateway.handle(req)
    log_case("POST /users selects the writer rule", req, chosen=result.rule_id)
    assert result.rule_id == "users-write"
