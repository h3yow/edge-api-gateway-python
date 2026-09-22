"""静态配置装载。

为保证“零外部依赖、本地可跑”，配置直接以 Python 数据结构描述；
端口可通过参数覆盖，便于与 :mod:`sim.local_run` 协同。

默认配置刻意包含三类可验证场景：

* 正常的前缀/方法/请求头路由（``/api/orders``、``/api/payments``）；
* 一个条件规则冲突演示（同时携带 ``x-beta: true`` 与 ``x-canary: true``
  命中两条同具体度规则，返回 ``ROUTE_AMBIGUOUS``）；
* 严格限流的调用方 ``demo-team``（小 burst / 小配额 / 小并发），
  以及宽松的 ``premium`` 调用方。
"""

from __future__ import annotations

from dataclasses import dataclass

from gateway.models import (
    CallerPolicy,
    Cluster,
    GatewayConfig,
    RouteRule,
    UpstreamInstance,
)


@dataclass(frozen=True)
class HealthSettings:
    failure_threshold: int = 3
    initial_cooldown_seconds: float = 0.5
    max_cooldown_seconds: float = 8.0
    probe_path: str = "/healthz"


def demo_config(
    port_map: dict[str, int] | None = None,
    *,
    request_timeout_seconds: float = 3.0,
) -> tuple[GatewayConfig, HealthSettings]:
    """构造默认演示配置。

    ``port_map`` 形如 ``{"orders-1": 9101, ...}``，缺省使用内置端口。
    """
    ports = dict(
        {
            "orders-1": 9101,
            "orders-2": 9102,
            "orders-3": 9103,
            "payments-1": 9201,
            "payments-2": 9202,
            "generic-1": 9301,
            "beta-1": 9401,
        }
    )
    if port_map:
        ports.update(port_map)

    def ins(name: str) -> UpstreamInstance:
        return UpstreamInstance(name, f"http://127.0.0.1:{ports[name]}")

    clusters = (
        Cluster("orders", (ins("orders-1"), ins("orders-2"), ins("orders-3"))),
        Cluster("payments", (ins("payments-1"), ins("payments-2"))),
        Cluster("generic", (ins("generic-1"),)),
        Cluster("beta", (ins("beta-1"),)),
    )

    rules = (
        # 具体度最高：路径2段 + 1个请求头约束
        RouteRule(
            "r-orders-acme",
            "/api/orders",
            "orders",
            frozenset(),
            (("x-tenant", "acme"),),
        ),
        # 路径2段 + 限定方法
        RouteRule(
            "r-orders-get",
            "/api/orders",
            "orders",
            frozenset(("GET",)),
        ),
        RouteRule(
            "r-orders-write",
            "/api/orders",
            "orders",
            frozenset(("POST", "PUT")),
        ),
        RouteRule(
            "r-payments-write",
            "/api/payments",
            "payments",
            frozenset(("POST", "PUT")),
        ),
        # 条件规则冲突演示：同一请求同时带 x-beta 与 x-canary 时两规则并列
        RouteRule(
            "r-orders-beta",
            "/api/orders",
            "beta",
            frozenset(),
            (("x-beta", "true"),),
        ),
        RouteRule(
            "r-orders-canary",
            "/api/orders",
            "payments",
            frozenset(),
            (("x-canary", "true"),),
        ),
        # 兜底：仅匹配 /api 根（空路径会匹配所有 URL，不能作为兜底）
        RouteRule("r-api-fallback", "/api", "generic"),
    )

    policies = {
        "demo-team": CallerPolicy(
            caller_id="demo-team",
            rate_per_second=2.0,
            burst=2.0,
            quota_limit=10,
            window_seconds=60.0,
            max_concurrency=2,
        ),
        "premium": CallerPolicy(
            caller_id="premium",
            rate_per_second=100.0,
            burst=100.0,
            quota_limit=100_000,
            window_seconds=60.0,
            max_concurrency=20,
        ),
    }

    config = GatewayConfig(
        rules=rules,
        clusters=clusters,
        caller_policies=policies,
        request_timeout_seconds=request_timeout_seconds,
    )
    return config, HealthSettings()
