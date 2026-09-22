"""请求转发与故障转移。

重试规则（可解释、可区分）：

* **幂等方法**（GET/HEAD/OPTIONS/PUT/DELETE）：连接错误、超时、5xx
  都会按候选顺序尝试下一个健康实例；
* **非幂等方法**（POST/PATCH 及其它）：**至多选择一个实例执行一次**。
  即使是连接拒绝这种“看起来没发出去”的错误，也无法在协议层证明服务端
  未执行（例如请求已到达、响应丢失），因此绝不自动故障转移重试，
  直接返回 ``UPSTREAM_UNAVAILABLE`` 并在 details 中说明
  ``non_idempotent_no_retry=true``，避免重复执行；
* 当集群所有实例都处于摘除状态时，先对到期实例触发**单飞探测**，
  探测恢复的实例可承载本次请求（探测是独立健康检查，不执行业务语义，
  所以对非幂等请求同样安全——这仍是它的第一次真实执行）；
* 4xx 视为上游正常产生的应用响应，直接透传，不重试，且上报健康成功。
"""

from __future__ import annotations

import time
from typing import Callable

import httpx

from gateway.errors import (
    GatewayError,
    upstream_error,
    upstream_timeout,
    upstream_unavailable,
)
from gateway.health import HealthRegistry
from gateway.models import (
    AttemptRecord,
    Cluster,
    DecisionLog,
    RequestFeature,
    UpstreamInstance,
    UpstreamResponse,
)
from gateway.pool import InstancePool

#: 默认可安全重试的幂等方法集合
IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})

ProbeFn = Callable[[UpstreamInstance], bool]


class Forwarder:
    def __init__(
        self,
        pool: InstancePool,
        registry: HealthRegistry,
        probe_fn: ProbeFn,
        *,
        timeout_seconds: float = 3.0,
        client: httpx.Client | None = None,
    ) -> None:
        self._pool = pool
        self._registry = registry
        self._probe = probe_fn
        self._timeout = timeout_seconds
        self._client = client or httpx.Client(timeout=timeout_seconds)

    def forward(
        self,
        feature: RequestFeature,
        cluster: Cluster,
        log: DecisionLog,
    ) -> UpstreamResponse:
        idempotent = feature.method.upper() in IDEMPOTENT_METHODS

        # 所有健康实例都已摘除时：先触发到期的单飞探测，再重取一次候选
        candidates = self._pool.candidates(cluster)
        if not candidates:
            self._probe_suspended(cluster)
            candidates = self._pool.candidates(cluster)
        log.candidate_instance_ids = [i.instance_id for i in candidates]
        if not candidates:
            healthy = self._pool.healthy_instances(cluster)
            raise upstream_unavailable(
                rule_id=cluster.name,
                tried=[a.instance_id for a in log.attempts],
                healthy=[i.instance_id for i in healthy],
            )

        ordered = candidates if idempotent else candidates[:1]
        last_record: _MutableAttempt | None = None
        for instance in ordered:
            log.chosen_instance_id = instance.instance_id
            record = self._attempt(feature, instance)
            last_record = record
            health = self._registry.get(instance.instance_id)
            log.attempts.append(record)

            if record.outcome in ("success", "http_4xx"):
                # 上游可达：清除连续失败计数
                self._registry.record_events(health.report_success())
                return UpstreamResponse(
                    status=record.status or 200,
                    body=record_detail_bytes(record),
                    headers=record.response_headers,
                    instance_id=instance.instance_id,
                    attempts=list(log.attempts),
                )

            # 失败：上报被动健康事件（达到阈值才会真正摘除）
            self._registry.record_events(
                health.report_failure(record.detail)
            )

            if not idempotent:
                # 关键保证：非幂等请求绝不尝试第二个实例
                raise _exhausted_error(
                    cluster, [i.instance_id for i in ordered], record,
                    non_idempotent=True, skipped=[
                        iid for iid in log.candidate_instance_ids
                        if iid != instance.instance_id
                    ],
                )
            # 幂等请求继续尝试下一个候选

        assert last_record is not None
        raise _exhausted_error(
            cluster, [i.instance_id for i in ordered], last_record,
            non_idempotent=False, skipped=[],
        )

    def _probe_suspended(self, cluster: Cluster) -> None:
        """对集群内摘除中的实例触发单飞探测（无候选时的恢复路径）。"""
        for instance in self._pool.all_instances_ordered(cluster):
            monitor = self._registry.get(instance.instance_id)
            self._registry.record_events(monitor.maybe_probe(self._probe))

    def _attempt(
        self, feature: RequestFeature, instance: UpstreamInstance
    ) -> "_MutableAttempt":
        url = instance.base_url.rstrip("/") + feature.path
        start = time.monotonic()
        try:
            response = self._client.request(
                feature.method.upper(),
                url,
                headers=dict(feature.headers),
                content=feature.body,
            )
        except httpx.TimeoutException as exc:
            return _MutableAttempt(
                instance,
                "timeout",
                None,
                time.monotonic() - start,
                f"{type(exc).__name__}: {exc}",
            )
        except httpx.TransportError as exc:  # 连接拒绝、连接重置等
            return _MutableAttempt(
                instance,
                "connection_error",
                None,
                time.monotonic() - start,
                f"{type(exc).__name__}: {exc}",
            )

        elapsed = time.monotonic() - start
        body = response.content
        headers = {
            k: v
            for k, v in response.headers.items()
            if k.lower() in ("content-type", "x-request-id")
        }
        outcome = (
            "success"
            if 200 <= response.status_code < 400
            else "http_4xx"
            if 400 <= response.status_code < 500
            else "http_5xx"
        )
        attempt = _MutableAttempt(
            instance, outcome, response.status_code, elapsed, "", body, headers
        )
        response.close()
        return attempt

    def close(self) -> None:
        self._client.close()


class _MutableAttempt(AttemptRecord):
    """附带响应体的内部尝试记录（响应体不进入判定日志）。"""

    def __init__(
        self,
        instance: UpstreamInstance,
        outcome: str,
        status: int | None,
        elapsed: float,
        detail: str,
        body: bytes = b"",
        response_headers: dict | None = None,
    ) -> None:
        super().__init__(
            instance_id=instance.instance_id,
            base_url=instance.base_url,
            outcome=outcome,
            status=status,
            elapsed_seconds=elapsed,
            detail=detail,
        )
        self.response_body = body
        self.response_headers = response_headers or {}


def record_detail_bytes(record: _MutableAttempt) -> bytes:
    return record.response_body


def _exhausted_error(
    cluster: Cluster,
    tried: list[str],
    last_record: _MutableAttempt,
    *,
    non_idempotent: bool,
    skipped: list[str],
) -> GatewayError:
    """根据最后一次尝试的结果选择可区分的最终错误码。"""
    if last_record.outcome == "timeout":
        error = upstream_timeout(
            f"upstream timeout after trying {tried}",
            {"rule_id": cluster.name, "tried_instances": tried},
        )
    elif last_record.outcome == "http_5xx":
        error = upstream_error(
            "upstream returned 5xx from all tried instances",
            {
                "rule_id": cluster.name,
                "tried_instances": tried,
                "last_status": last_record.status,
            },
        )
    else:
        error = upstream_unavailable(cluster.name, tried, healthy=[])

    if non_idempotent:
        # 关键语义标记：本次失败没有对剩余候选执行第二次请求
        error.details["non_idempotent_no_retry"] = True
        error.details["skipped_remaining_candidates"] = skipped
    return error
