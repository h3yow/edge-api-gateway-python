"""健康摘除、故障转移、恢复与“非幂等不重复执行”集成测试。

使用真实模拟上游（HTTP 500、停机拒绝连接、慢响应），
退避/恢复时刻由假时钟驱动，结论稳定可复现。
"""

from __future__ import annotations

from gateway.config import HealthSettings
from gateway.errors import ErrorCode, GatewayError
from gateway.models import RequestFeature

from conftest import concurrent_call


def _settings(**over):
    base = dict(
        failure_threshold=1,
        initial_cooldown_seconds=0.5,
        max_cooldown_seconds=8.0,
        probe_path="/healthz",
    )
    base.update(over)
    return HealthSettings(**base)


def _req(method="GET", path="/api/orders", caller="premium", **headers):
    h = {f"X-{k.replace('_', '-').title()}": v for k, v in headers.items()}
    h["X-Caller-Id"] = caller
    return RequestFeature(method=method, path=path, headers=h)


def _print(title, feature, verdict, basis):
    print(
        f"\n[{title}] 输入: {feature.method} {feature.path} {feature.headers}\n"
        f"  判定依据: {basis}\n  最终选择: {verdict}"
    )


def test_idempotent_get_fails_over_after_5xx_and_suspends(
    gateway_factory, cluster
):
    gw, clock = gateway_factory(health=_settings())
    cluster.set_behaviour("orders-1", "http_500")

    # 首个候选恰好为轮转起点 orders-1（第一次请求 shift=0）
    response = gw.handle(_req())
    snap = gw.health_snapshot()
    state_i1 = next(i for i in snap["instances"] if i["instance_id"] == "orders-1")
    _print("failover", _req(),
           f"served_by={response.instance_id} status={response.status}",
           {"orders-1": state_i1["state"],
            "attempts": [a.to_dict() for a in response.attempts]})
    # GET 幂等：orders-1 失败后故障转移到健康实例
    assert response.status == 200
    assert response.instance_id != "orders-1"
    assert state_i1["state"] == "SUSPENDED"

    # 之后的请求不再选择 orders-1
    served = {gw.handle(_req()).instance_id for _ in range(6)}
    assert "orders-1" not in served
    assert served <= {"orders-2", "orders-3"}


def test_suspended_instance_recovers_after_probe(gateway_factory, cluster):
    gw, clock = gateway_factory(health=_settings())
    cluster.set_behaviour("orders-1", "http_500")
    gw.handle(_req())  # 触发摘除
    assert gw.registry.get("orders-1").snapshot()["state"] == "SUSPENDED"

    # 上游修复
    cluster.set_behaviour("orders-1", "ok")
    # 未到退避时间：探测不发生，仍摘除
    clock.advance(0.4)
    gw.probe_all()
    assert gw.registry.get("orders-1").snapshot()["state"] == "SUSPENDED"

    # 退避到期：单飞探测成功 -> 恢复
    clock.advance(0.2)
    events = gw.probe_all()
    recovered = [e["transition"] for e in events if e["instance_id"] == "orders-1"]
    _print("recover", _req(), recovered,
           "退避0.5s到期后探测 /healthz=200，恢复 HEALTHY 并重新纳入")
    assert recovered == ["recover"]
    assert gw.registry.get("orders-1").snapshot()["state"] == "HEALTHY"

    # 恢复后重新承载流量（连续多次轮转必然再次选中它）
    served = {gw.handle(_req()).instance_id for _ in range(9)}
    assert "orders-1" in served


def test_connection_refused_is_failover_and_suspend(gateway_factory, cluster):
    gw, clock = gateway_factory(health=_settings())
    cluster.get("orders-1").stop()  # 端口拒绝连接

    response = gw.handle(_req())
    attempts = [(a.instance_id, a.outcome) for a in response.attempts]
    state_i1 = gw.registry.get("orders-1").snapshot()
    _print("conn-refused", _req(),
           f"served_by={response.instance_id}",
           {"attempts": attempts, "orders-1_state": state_i1["state"]})
    assert response.status == 200
    assert attempts[0] == ("orders-1", "connection_error")
    assert state_i1["state"] == "SUSPENDED"

    # 实例仍停机：到期探测失败，保持摘除（无状态抖动）
    clock.advance(1.0)
    gw.probe_all()
    assert gw.registry.get("orders-1").snapshot()["state"] == "SUSPENDED"

    # 重启实例：下一轮探测恢复
    cluster.get("orders-1").restart()
    clock.advance(1.0)
    gw.probe_all()
    assert gw.registry.get("orders-1").snapshot()["state"] == "HEALTHY"


def test_non_idempotent_post_never_executed_twice(gateway_factory, cluster):
    gw, clock = gateway_factory(health=_settings())
    cluster.set_behaviour("orders-1", "http_500")

    req = _req("POST", "/api/orders")
    with __import__("pytest").raises(GatewayError) as exc:
        gw.handle(req)
    err = exc.value
    _print("non-idempotent", req, err.code.value,
           {"details": err.details})

    # 可区分错误 + 明确标注未对其他候选重试
    assert err.code in (
        ErrorCode.UPSTREAM_ERROR, ErrorCode.UPSTREAM_UNAVAILABLE
    )
    assert err.details["non_idempotent_no_retry"] is True

    counts = {iid: cluster.state(iid)["method_counts"].get("POST", 0)
              for iid in ("orders-1", "orders-2", "orders-3")}
    print("  各实例 POST 实际执行次数:", counts)
    assert counts == {"orders-1": 1, "orders-2": 0, "orders-3": 0}

    # 再次请求：orders-1 已摘除，请求在健康实例上成功；
    # 新请求本身也只执行一次
    response = gw.handle(req)
    assert response.status == 200
    counts2 = {iid: cluster.state(iid)["method_counts"].get("POST", 0)
               for iid in ("orders-1", "orders-2", "orders-3")}
    print("  第二次请求后 POST 执行次数:", counts2)
    assert counts2["orders-1"] == 1  # 没有对摘除实例二次执行
    assert sum(counts2.values()) == 2


def test_all_instances_down_returns_distinct_unavailable(gateway_factory, cluster):
    gw, clock = gateway_factory(health=_settings())
    for iid in ("orders-1", "orders-2", "orders-3"):
        cluster.get(iid).stop()

    with __import__("pytest").raises(GatewayError) as exc:
        gw.handle(_req())
    _print("all-down", _req(), exc.value.code.value, exc.value.details)
    assert exc.value.code is ErrorCode.UPSTREAM_UNAVAILABLE


def test_concurrent_requests_no_duplicate_suspend_events(gateway_factory, cluster):
    """并发下 orders-1 持续 500：摘除事件必须只出现一次，状态不抖动。"""
    settings = HealthSettings(
        failure_threshold=3,
        initial_cooldown_seconds=100.0,  # 测试期间不触发探测恢复
        max_cooldown_seconds=200.0,
    )
    gw, clock = gateway_factory(health=settings)
    cluster.set_behaviour("orders-1", "http_500")

    def one(i):
        try:
            r = gw.handle(_req(path=f"/api/orders/{i}"))
            return ("ok", r.instance_id)
        except GatewayError as exc:
            return ("err", exc.code.value)

    results = concurrent_call(one, 12)
    suspends = [
        e for e in gw.health_snapshot()["events"]
        if e["instance_id"] == "orders-1" and e["transition"] == "suspend"
    ]
    recovers = [
        e for e in gw.health_snapshot()["events"]
        if e["instance_id"] == "orders-1" and e["transition"] == "recover"
    ]
    print("\n[concurrent-suspend] 结果分布:",
          {v: [r[1] for r in results].count(v)
           for v in {r[1] for r in results}})
    print("  判定依据: 摘除事件数 =", len(suspends),
          "恢复事件数 =", len(recovers))
    assert len(suspends) == 1
    assert recovers == []
