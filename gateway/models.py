"""网关领域模型。

只用标准库 dataclass，避免引入额外抽象；所有模型均为不可变值对象，
方便在多线程/并发测试中安全共享。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class RequestFeature:
    """从入口请求中提取出的路由特征。"""

    method: str
    path: str
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""

    def header(self, name: str) -> str | None:
        lowered = name.lower()
        for key, value in self.headers.items():
            if key.lower() == lowered:
                return value
        return None


@dataclass(frozen=True)
class RouteRule:
    """一条路由规则。

    匹配条件（全部满足才算命中）：

    * ``path_prefix``：请求路径按 ``/`` 分段前缀匹配；
    * ``methods``：若非空，请求方法必须在其中；
    * ``require_headers``：若非空，指定请求头必须全部相等（头名大小写不敏感）。
    """

    rule_id: str
    path_prefix: str
    cluster: str
    methods: frozenset[str] = frozenset()
    require_headers: tuple[tuple[str, str], ...] = ()

    def matches(self, feature: RequestFeature) -> bool:
        method = feature.method.upper()
        if self.methods and method not in {m.upper() for m in self.methods}:
            return False
        if not _path_prefix_match(feature.path, self.path_prefix):
            return False
        for name, expected in self.require_headers:
            actual = feature.header(name)
            if actual is None or actual != expected:
                return False
        return True

    def specificity(self) -> tuple[int, int, int]:
        """规则具体度（越大越优先）。

        依次比较：路径段数、约束的请求头数、是否限定方法。
        """
        return (
            len([seg for seg in self.path_prefix.split("/") if seg]),
            len(self.require_headers),
            1 if self.methods else 0,
        )


def _path_prefix_match(path: str, prefix: str) -> bool:
    path_segments = [seg for seg in path.split("/") if seg]
    prefix_segments = [seg for seg in prefix.split("/") if seg]
    if len(prefix_segments) > len(path_segments):
        return False
    return path_segments[: len(prefix_segments)] == prefix_segments


@dataclass(frozen=True)
class UpstreamInstance:
    """一个上游实例。"""

    instance_id: str
    base_url: str  # 形如 http://127.0.0.1:9001


@dataclass(frozen=True)
class Cluster:
    """上游集群：包含若干同构实例。"""

    name: str
    instances: tuple[UpstreamInstance, ...]


@dataclass(frozen=True)
class CallerPolicy:
    """调用方维度的限流/配额策略。

    * ``rate_per_second``：令牌桶填充速率（个/秒），桶容量为 ``burst``；
    * ``quota_limit`` 与 ``window_seconds``：固定窗口总配额；
    * ``max_concurrency``：同时在途请求上限。
    """

    caller_id: str
    rate_per_second: float
    burst: float
    quota_limit: int
    window_seconds: float
    max_concurrency: int


@dataclass(frozen=True)
class GatewayConfig:
    """网关静态配置。"""

    rules: tuple[RouteRule, ...]
    clusters: tuple[Cluster, ...]
    caller_policies: dict[str, CallerPolicy]
    request_timeout_seconds: float = 3.0

    def cluster(self, name: str) -> Cluster | None:
        for cluster in self.clusters:
            if cluster.name == name:
                return cluster
        return None


@dataclass
class AttemptRecord:
    """一次上游尝试的记录（用于可解释日志）。"""

    instance_id: str
    base_url: str
    outcome: str  # "success" | "connection_error" | "http_5xx" | "http_4xx" | "skipped"
    status: int | None = None
    elapsed_seconds: float = 0.0
    detail: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "instance_id": self.instance_id,
            "base_url": self.base_url,
            "outcome": self.outcome,
            "status": self.status,
            "elapsed_seconds": round(self.elapsed_seconds, 4),
            "detail": self.detail,
        }


@dataclass
class DecisionLog:
    """单次请求的完整判定记录。"""

    feature: RequestFeature
    matched_rule_id: str | None = None
    matched_on: dict[str, Any] = field(default_factory=dict)
    candidate_instance_ids: list[str] = field(default_factory=list)
    chosen_instance_id: str | None = None
    attempts: list[AttemptRecord] = field(default_factory=list)
    rejected_code: str | None = None
    rejected_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "request": {
                "method": self.feature.method,
                "path": self.feature.path,
                "headers": dict(self.feature.headers),
            },
            "matched_rule_id": self.matched_rule_id,
            "matched_on": self.matched_on,
            "candidate_instance_ids": list(self.candidate_instance_ids),
            "chosen_instance_id": self.chosen_instance_id,
            "attempts": [a.to_dict() for a in self.attempts],
            "rejected_code": self.rejected_code,
            "rejected_reason": self.rejected_reason,
        }


@dataclass
class UpstreamResponse:
    """转发成功后的上游响应。"""

    status: int
    body: bytes
    headers: dict[str, str]
    instance_id: str
    attempts: list[AttemptRecord]
    rule_id: str = ""
