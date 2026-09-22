"""规则匹配优先级、冲突与跨入口确定性测试。"""

from __future__ import annotations

import random

import pytest

from gateway.errors import ErrorCode, GatewayError
from gateway.models import RequestFeature, RouteRule
from gateway.rules import RuleIndex


def _print(title: str, feature: RequestFeature, result: str, basis) -> None:
    print(
        f"\n[{title}] 输入: method={feature.method} path={feature.path} "
        f"headers={feature.headers}\n  判定依据: {basis}\n  最终选择: {result}"
    )


def test_specificity_ordering():
    generic = RouteRule("r-generic", "/api", "g")
    orders = RouteRule("r-orders", "/api/orders", "o")
    post = RouteRule("r-orders-post", "/api/orders", "o", frozenset(["POST"]))
    tenant = RouteRule(
        "r-orders-acme", "/api/orders", "o", frozenset(),
        (("x-tenant", "acme"),),
    )
    index = RuleIndex((generic, orders, post, tenant))

    # GET /api/orders -> 路径2段无方法约束：r-orders 胜出（r-orders-post 限 POST）
    f = RequestFeature("GET", "/api/orders", {})
    outcome = index.match(f)
    _print("specificity", f, outcome.rule.rule_id,
           {"rank": outcome.rank, "all_hits": outcome.all_hit_rule_ids})
    assert outcome.rule.rule_id == "r-orders"

    # POST + tenant 头 -> 路径2段+1头约束(3级) 胜 路径2段+方法约束
    f2 = RequestFeature("POST", "/api/orders/1", {"X-Tenant": "acme"})
    outcome2 = index.match(f2)
    _print("specificity", f2, outcome2.rule.rule_id,
           {"rank": outcome2.rank, "all_hits": outcome2.all_hit_rule_ids})
    assert outcome2.rule.rule_id == "r-orders-acme"


def test_no_match_returns_distinct_error():
    index = RuleIndex((RouteRule("r", "/api", "g"),))
    f = RequestFeature("GET", "/v2/unknown", {})
    with pytest.raises(GatewayError) as exc:
        index.match(f)
    _print("no_match", f, exc.value.code.value, exc.value.details)
    assert exc.value.code is ErrorCode.ROUTE_NO_MATCH


def test_equal_specificity_conflicts_not_silently_picked():
    a = RouteRule("a-rule", "/api/orders", "c1", frozenset(),
                  (("x-beta", "true"),))
    b = RouteRule("b-rule", "/api/orders", "c2", frozenset(),
                  (("x-canary", "true"),))
    index = RuleIndex((a, b))
    f = RequestFeature("GET", "/api/orders",
                       {"X-Beta": "true", "X-Canary": "true"})
    with pytest.raises(GatewayError) as exc:
        index.match(f)
    _print("conflict", f, exc.value.code.value, exc.value.details)
    assert exc.value.code is ErrorCode.ROUTE_AMBIGUOUS
    assert exc.value.details["conflicting_rule_ids"] == ["a-rule", "b-rule"]

    # 只有一个头时不冲突，且结果确定
    only_a = RequestFeature("GET", "/api/orders", {"X-Beta": "true"})
    assert index.match(only_a).rule.rule_id == "a-rule"


def test_result_independent_of_rule_insertion_order():
    rules = [
        RouteRule("r-generic", "/api", "g"),
        RouteRule("r-orders", "/api/orders", "o"),
        RouteRule("r-orders-post", "/api/orders", "o", frozenset(["POST"])),
    ]
    feature_ = RequestFeature("POST", "/api/orders", {})
    winners = set()
    for seed in range(20):
        shuffled = rules[:]
        random.Random(seed).shuffle(shuffled)
        winners.add(RuleIndex(shuffled).match(feature_).rule.rule_id)
    _print("determinism", feature_, winners,
           "20 次随机规则顺序下的命中集合")
    assert winners == {"r-orders-post"}
