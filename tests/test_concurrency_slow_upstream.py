"""上游变慢场景下的并发一致性。

场景：

* ``max_concurrency=2``，上游人为变慢 -> 并发 10 个请求中
  至多 2 个真正在途，其余必须以 CONCURRENCY_LIMITED 拒绝；
* 被拒请求不增加任何上游实例的执行计数（不占上游资源）；
* 在途请求结束后槽位释放，后续请求恢复。
"""

from __future__ import annotations

import threading
import time
from dataclasses import replace

from gateway.errors import GatewayError
from gateway.models import CallerPolicy, RequestFeature

from conftest import concurrent_call


def _slow_policy_config(cluster):
    policies = dict(cluster.config.caller_policies)
    policies["slow"] = CallerPolicy(
        "slow", rate_per_second=10_000.0, burst=10_000.0,
        quota_limit=1000, window_seconds=60.0, max_concurrency=2,
    )
    return replace(cluster.config, caller_policies=policies)


def test_concurrency_cap_holds_with_slow_upstream(gateway_factory, cluster):
    config = _slow_policy_config(cluster)
    from gateway.config import HealthSettings
    from gateway.proxy import Gateway

    gw, _clock = gateway_factory()
    # 用带严格并发策略的配置重建网关
    gw.close()
    gw = Gateway(
        config,
        HealthSettings(failure_threshold=5),
    )

    # 让所有 orders 实例变慢（0.4s）
    for iid in ("orders-1", "orders-2", "orders-3"):
        cluster.set_behaviour(iid, "slow", seconds=0.4)

    inflight = {"max_seen": 0}
    sampling = {"stop": False}
    slock = threading.Lock()

    def sampler():
        # 从网关侧采样真实在途数（被并发限制拒绝的请求不计入）
        while not sampling["stop"]:
            try:
                value = gw.caller_snapshot("slow")["in_flight"]
                with slock:
                    inflight["max_seen"] = max(inflight["max_seen"], value)
            except Exception:
                pass
            time.sleep(0.005)

    monitor = threading.Thread(target=sampler, daemon=True)
    monitor.start()

    def one(i):
        req = RequestFeature(
            "GET", f"/api/orders/{i}", {"X-Caller-Id": "slow"}
        )
        try:
            r = gw.handle(req)
            return ("accepted", str(r.status))
        except GatewayError as exc:
            return ("rejected", exc.code.value)

    try:
        results = concurrent_call(one, 10)
    finally:
        sampling["stop"] = True
        monitor.join(timeout=1)
    accepted = [r for r in results if r[0] == "accepted"]
    rejected = {}
    for _, code in (r for r in results if r[0] == "rejected"):
        rejected[code] = rejected.get(code, 0) + 1

    upstream_hits = sum(
        cluster.state(iid)["total"]
        for iid in ("orders-1", "orders-2", "orders-3")
    )
    print("\n[slow-upstream] 接受:", len(accepted), "拒绝:", rejected)
    print("  判定依据: 网关观测到的最大同时在途 =", inflight["max_seen"],
          "(注意：被并发限制拒绝的请求在进入上游前已返回)")
    print("  上游实际收到请求总数:", upstream_hits,
          "= 接受数", len(accepted))
    assert len(accepted) == 2
    assert rejected == {"CONCURRENCY_LIMITED": 8}
    assert upstream_hits == len(accepted)
    assert inflight["max_seen"] <= 2

    # 等待在途结束、槽位释放后恢复（连接级客户端，请求已随线程结束）
    time.sleep(0.1)
    req = RequestFeature("GET", "/api/orders/recovered",
                         {"X-Caller-Id": "slow"})
    # 关闭慢模式以便快速完成
    for iid in ("orders-1", "orders-2", "orders-3"):
        cluster.set_behaviour(iid, "ok")
    response = gw.handle(req)
    print("  恢复后请求状态:", response.status,
          "实例:", response.instance_id)
    assert response.status == 200
    gw.close()


def test_quota_consumed_exactly_once_per_accepted_request(gateway_factory):
    """即使并发竞争，配额扣减次数 == 被接受请求数，绝不双扣/漏扣。"""
    from gateway.models import CallerPolicy
    from gateway.proxy import Gateway
    from gateway.config import HealthSettings

    gw, clock = gateway_factory()
    gw.close()
    # 独立小集群配置不必要；直接用宽松速率 + 小配额的调用方验证记账
    rl = gw.limiter
    from gateway.ratelimit import RateLimiter
    policy = CallerPolicy("q", 10_000.0, 10_000.0, 25, 60.0, 10_000)
    rl2 = RateLimiter({"q": policy}, time_func=clock)

    accepted_box: list = []

    def one(i):
        try:
            t = rl2.admit("q")
            accepted_box.append(t)
            return "ok"
        except GatewayError as exc:
            return exc.code.value

    results = concurrent_call(one, 60)
    ok = [r for r in results if r == "ok"]
    rejected = [r for r in results if r != "ok"]
    print("\n[quota-accounting] 接受:", len(ok),
          "拒绝:", {c: rejected.count(c) for c in set(rejected)})
    print("  判定依据: quota_used =", rl2.snapshot("q")["quota_used"])
    assert len(ok) == 25
    assert rl2.snapshot("q")["quota_used"] == 25
    for t in accepted_box:
        t.release()
