"""通过 FastAPI 统一入口的端到端 HTTP 行为测试。"""

from __future__ import annotations

import time
from dataclasses import replace

import pytest
from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.models import CallerPolicy


@pytest.fixture
def client(cluster):
    app = create_app(
        cluster.config,
        cluster.health_settings,
        enable_probe_loop=True,
        probe_interval=0.05,
    )
    with TestClient(app) as c:
        yield c


def _orders_get_count(cluster) -> int:
    return sum(
        cluster.state(iid)["method_counts"].get("GET", 0)
        for iid in ("orders-1", "orders-2", "orders-3")
    )


def test_success_response_explains_rule_and_instance(client):
    r = client.get("/api/orders/7", headers={"X-Caller-Id": "premium"})
    print("\n[http-success] status:", r.status_code,
          "rule:", r.headers.get("X-Gateway-Rule"),
          "instance:", r.headers.get("X-Gateway-Instance"))
    assert r.status_code == 200
    assert r.headers["X-Gateway-Rule"] == "r-orders-get"
    assert r.headers["X-Gateway-Instance"].startswith("orders-")


def test_distinct_error_codes_over_http(client):
    cases = [
        ("no-caller", {}, 401, "CALLER_UNIDENTIFIED"),
        ("no-route", {"X-Caller-Id": "premium"}, 404, "ROUTE_NO_MATCH"),
    ]
    for path_extra, hdr, want_status, want_code in cases:
        path = "/api/orders" if path_extra == "no-caller" else "/does/not/exist"
        r = client.get(path, headers=hdr)
        print(f"\n[http-{want_code}] {r.status_code} {r.json()}")
        assert r.status_code == want_status
        assert r.json()["error"]["code"] == want_code

    r = client.get(
        "/api/orders",
        headers={"X-Caller-Id": "premium", "X-Beta": "true", "X-Canary": "true"},
    )
    print("\n[http-ambiguous]", r.status_code, r.json()["error"]["details"])
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "ROUTE_AMBIGUOUS"


def test_rate_limited_request_does_not_consume_upstream(client, cluster):
    headers = {"X-Caller-Id": "demo-team"}  # burst=2, rate=2/s
    statuses = []
    for i in range(3):
        r = client.get(f"/api/orders/{i}", headers=headers)
        statuses.append(r.status_code)
        print(f"\n[http-burst] 请求{i+1}: {r.status_code} "
              f"{r.json().get('error', {}).get('code', 'OK')}")
    assert statuses[0] == 200
    assert statuses[1] == 200
    assert statuses[2] == 429
    assert r.json()["error"]["code"] == "RATE_LIMITED"

    before = _orders_get_count(cluster)
    rejected = client.get("/api/orders/zzz", headers=headers)
    after = _orders_get_count(cluster)
    print("\n[http-rejected-no-upstream] 拒绝码:",
          rejected.json()["error"]["code"],
          "上游计数 before/after:", before, after)
    assert rejected.status_code == 429
    assert before == after  # 被拒请求未触达任何上游


def test_quota_boundary_over_http(cluster):
    # 构造严格配额的调用方：quota=3，burst=10，避免速率先触发
    policies = dict(cluster.config.caller_policies)
    policies["tight"] = CallerPolicy(
        "tight", rate_per_second=1000.0, burst=1000.0,
        quota_limit=3, window_seconds=60.0, max_concurrency=10,
    )
    config = replace(cluster.config, caller_policies=policies)
    app = create_app(config, cluster.health_settings, enable_probe_loop=False)
    with TestClient(app) as c:
        codes = []
        for i in range(5):
            r = c.get(f"/api/orders/{i}", headers={"X-Caller-Id": "tight"})
            codes.append((r.status_code,
                          r.json().get("error", {}).get("code", "OK")))
        print("\n[http-quota] 5 次请求结果:", codes)
        accepted = [s for s, _ in codes if s == 200]
        rejected = [code for _, code in codes if code == "QUOTA_EXHAUSTED"]
        assert len(accepted) == 3
        assert len(rejected) == 2
        # 配额视图
        view = c.get("/_gw/callers/tight").json()
        print("  调用方配额视图:", {k: view[k] for k in
              ("quota_used", "quota_limit", "quota_remaining")})
        assert view["quota_used"] == 3
