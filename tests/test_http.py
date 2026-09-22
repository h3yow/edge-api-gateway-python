"""HTTP surface: unified entry, entry-independence, error codes, admin API."""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from conftest import log_case
from edge_gateway.app import create_app


@pytest.fixture()
def client():
    app = create_app()
    return TestClient(app)


def test_healthz(client):
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


def test_same_request_same_result_from_both_entries(client):
    h = {"x-caller-id": "alice", "x-canary": "true"}
    r1 = client.get("/gateway/users/3", headers=h)
    r2 = client.get("/users/3", headers=h)
    log_case(
        "two entry paths, one decision",
        chosen={"via_gateway": r1.json()["rule_id"], "via_root": r2.json()["rule_id"]},
    )
    assert r1.status_code == r2.status_code == 200
    assert r1.json()["rule_id"] == r2.json()["rule_id"] == "users-canary"
    assert r1.json()["instance_id"] == r2.json()["instance_id"]


def test_response_is_explainable(client):
    resp = client.get("/users/5", headers={"x-caller-id": "bob"})
    body = resp.json()
    log_case(
        "success response carries rule, instance, quota and trace",
        chosen={k: body[k] for k in ("rule_id", "cluster", "instance_id")},
        quota=body["quota"],
        trace=body["trace"],
    )
    assert body["rule_id"] == "users-read"
    assert body["instance_id"] in ("users-v1-1", "users-v1-2")
    assert body["trace"], "decision basis must be present"


def test_no_route_error_code(client):
    resp = client.get("/nothing/here", headers={"x-caller-id": "bob"})
    log_case("HTTP no-route", error=resp.json())
    assert resp.status_code == 404
    assert resp.json()["error"]["code"] == "NO_ROUTE"


def test_conflict_error_code(client):
    resp = client.get("/conflict", headers={"x-experiment": "A", "x-caller-id": "b"})
    log_case("HTTP conflict", error=resp.json()["error"])
    assert resp.status_code == 409
    assert resp.json()["error"]["code"] == "ROUTE_CONFLICT"


def test_quota_error_code_and_no_upstream_attempts(client):
    headers = {"x-caller-id": "quota-http"}
    statuses = [client.get("/orders/1", headers=headers).status_code for _ in range(5)]
    rejected = client.get("/orders/1", headers=headers)
    body = rejected.json()
    log_case(
        "HTTP quota exhaustion",
        statuses=statuses,
        rejected_body=body,
    )
    assert statuses.count(200) == 4
    assert rejected.status_code == 429
    assert body["error"]["code"] == "QUOTA_EXCEEDED"
    assert body["error"]["details"]["quota"]["used"] == 4


def test_admin_fault_injection_and_failover_over_http(client):
    inject = client.post(
        "/__admin/faults",
        json={"instance_id": "users-v1-1", "fault": "fail"},
    )
    assert inject.json()["ok"] is True
    resp = client.get("/users/9", headers={"x-caller-id": "http-fail"})
    body = resp.json()
    log_case(
        "HTTP failover after injected fault",
        attempts=body["attempts"],
        final=body["instance_id"],
    )
    assert body["instance_id"] == "users-v1-2"
    assert [a["outcome"] for a in body["attempts"]] == ["http_5xx", "success"]


def test_admin_reset_reopens_and_clears(client):
    client.post("/__admin/faults", json={"instance_id": "users-v1-1", "fault": "down"})
    for _ in range(3):
        client.get("/users/1", headers={"x-caller-id": "reset-me"})
    state = client.get("/__admin/state").json()
    assert state["health"]["users-v1-1"]["state"] == "OPEN"
    client.post("/__admin/reset")
    state2 = client.get("/__admin/state").json()
    log_case("admin reset restores health", before=state["health"]["users-v1-1"], after=state2["health"]["users-v1-1"])
    assert state2["health"]["users-v1-1"]["state"] == "CLOSED"
    resp = client.get("/users/1", headers={"x-caller-id": "reset-me"})
    assert resp.json()["instance_id"] == "users-v1-1"


def test_explain_endpoint_does_not_dispatch(client):
    resp = client.get("/__explain", params={"path": "/users/1", "method": "GET"}, headers={"x-canary": "true"})
    body = resp.json()
    log_case("explain endpoint", routing_decision=body["routing"]["decision"], selected=body["routing"]["selected_rule_id"])
    assert body["routing"]["selected_rule_id"] == "users-canary"
