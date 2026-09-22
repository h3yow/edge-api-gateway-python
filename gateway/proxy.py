"""网关编排核心。

处理顺序是固定的，且每一步的输入与依据都会进入 :class:`DecisionLog`：

1. 识别调用方（``X-Caller-Id``，未知/缺失 -> ``CALLER_UNIDENTIFIED``）；
2. 规则匹配（无规则 -> ``ROUTE_NO_MATCH``；并列冲突 -> ``ROUTE_AMBIGUOUS``）；
3. 调用方限流/配额/并发**原子预占**——被拒绝的请求在这一步直接返回，
   不会选择上游，也不会发起任何上游连接；
4. 在健康实例中按确定顺序选择并转发；幂等请求可故障转移，
   非幂等请求至多执行一次；
5. 无论成功失败都释放预占的在途槽位（速率令牌与配额不回补，
   “被接受过”本身就计入配额）。

健康探测注入为 httpx 调用 ``GET {base_url}/healthz``；后台周期性
:meth:`Gateway.probe_all` 可由 FastAPI 启动事件驱动。
"""

from __future__ import annotations

import logging
import threading
from typing import Callable

import httpx

from gateway.config import HealthSettings
from gateway.forwarder import Forwarder
from gateway.health import HealthRegistry, InstanceHealth
from gateway.models import (
    CallerPolicy,
    DecisionLog,
    GatewayConfig,
    RequestFeature,
    UpstreamInstance,
    UpstreamResponse,
)
from gateway.pool import InstancePool
from gateway.ratelimit import RateLimiter
from gateway.router import Router

logger = logging.getLogger("gateway")

CALLER_HEADER = "x-caller-id"


class Gateway:
    def __init__(
        self,
        config: GatewayConfig,
        health_settings: HealthSettings | None = None,
        *,
        time_func: Callable[[], float] | None = None,
        probe_client: httpx.Client | None = None,
        forwarder: Forwarder | None = None,
        rate_limiter: RateLimiter | None = None,
    ) -> None:
        self.config = config
        self.health_settings = health_settings or HealthSettings()
        self.router = Router(config)
        clock = time_func
        self.limiter = rate_limiter or RateLimiter(
            config.caller_policies,
            **({"time_func": clock} if clock is not None else {}),
        )

        self.registry = HealthRegistry()
        for cluster in config.clusters:
            for instance in cluster.instances:
                self.registry.monitors[instance.instance_id] = InstanceHealth(
                    instance,
                    failure_threshold=self.health_settings.failure_threshold,
                    initial_cooldown=self.health_settings.initial_cooldown_seconds,
                    max_cooldown=self.health_settings.max_cooldown_seconds,
                    **({"time_func": clock} if clock is not None else {}),
                )

        self.pool = InstancePool(self.registry)
        self._probe_client = probe_client or httpx.Client(timeout=1.0)
        self._owns_probe_client = probe_client is None
        probe_fn = self._http_probe
        self.forwarder = forwarder or Forwarder(
            self.pool,
            self.registry,
            probe_fn,
            timeout_seconds=config.request_timeout_seconds,
        )

    # ---- 健康探测 ----
    def _http_probe(self, instance: UpstreamInstance) -> bool:
        url = instance.base_url.rstrip("/") + self.health_settings.probe_path
        try:
            response = self._probe_client.get(url)
        except httpx.HTTPError:
            return False
        ok = 200 <= response.status_code < 400
        response.close()
        return ok

    def probe_all(self) -> list[dict]:
        """对所有到期实例执行一次单飞探测，返回本次状态事件。"""
        return [
            {
                "instance_id": e.instance_id,
                "transition": e.transition,
                "at": round(e.at, 4),
            }
            for e in self.registry.probe_due(self._http_probe)
        ]

    # ---- 主入口 ----
    def handle(self, feature: RequestFeature) -> UpstreamResponse:
        log = DecisionLog(feature=feature)
        ticket = None
        try:
            caller = feature.header(CALLER_HEADER)
            log.matched_on["caller_id"] = caller

            # 1) 调用方识别（未知调用方不计入任何配额）
            if not caller or caller not in self.config.caller_policies:
                from gateway.errors import caller_unidentified

                raise caller_unidentified()

            # 2) 确定性路由
            decision = self.router.route(feature)
            log.matched_rule_id = decision.rule.rule_id
            log.matched_on["rule"] = {
                "rule_id": decision.rule.rule_id,
                "path_prefix": decision.rule.path_prefix,
                "methods": sorted(decision.rule.methods),
                "require_headers": [list(h) for h in decision.rule.require_headers],
                "all_hit_rule_ids": list(decision.all_hit_rule_ids),
            }
            log.matched_on["cluster"] = decision.cluster.name

            # 3) 限流原子预占（失败即返回，绝不触碰上游）
            ticket = self.limiter.admit(caller)
            log.matched_on["admission"] = self.limiter.snapshot(caller)

            # 4) 转发 / 故障转移
            response = self.forwarder.forward(feature, decision.cluster, log)
            response.rule_id = decision.rule.rule_id
            self._log_decision(log, caller, "accepted")
            return response
        except Exception as exc:
            self._annotate_rejection(log, exc)
            self._log_decision(
                log,
                getattr(ticket, "caller_id", feature.header(CALLER_HEADER)),
                "rejected" if log.rejected_code else "error",
            )
            raise
        finally:
            # 5) 释放在途槽位（恰好一次）；令牌与配额不回补
            if ticket is not None:
                ticket.release()

    # ---- 可观测性 ----
    def health_snapshot(self) -> dict:
        return {
            "instances": [m.snapshot() for m in self.registry.monitors.values()],
            "events": self.registry.event_log(),
        }

    def caller_snapshot(self, caller: str) -> dict:
        return self.limiter.snapshot(caller)

    def explain_rules(self, feature: RequestFeature) -> list[dict]:
        return self.router.index.explain(feature)

    @staticmethod
    def _annotate_rejection(log: DecisionLog, exc: Exception) -> None:
        from gateway.errors import GatewayError

        if isinstance(exc, GatewayError):
            log.rejected_code = exc.code.value
            log.rejected_reason = exc.message

    @staticmethod
    def _log_decision(log: DecisionLog, caller: object, verdict: str) -> None:
        payload = log.to_dict()
        logger.info(
            "gateway decision verdict=%s caller=%s %s",
            verdict,
            caller,
            _compact_json(payload),
        )

    def close(self) -> None:
        if self._owns_probe_client:
            self._probe_client.close()
        self.forwarder.close()


def _compact_json(value: object) -> str:
    import json

    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
