"""路由器：在规则索引之上把命中规则解析到具体上游集群。"""

from __future__ import annotations

from dataclasses import dataclass

from gateway.models import Cluster, GatewayConfig, RequestFeature, RouteRule
from gateway.rules import MatchOutcome, RuleIndex


@dataclass(frozen=True)
class RouteDecision:
    feature: RequestFeature
    rule: RouteRule
    cluster: Cluster
    all_hit_rule_ids: tuple[str, ...]


class Router:
    """无状态路由器，配置注入后可被并发共享。"""

    def __init__(self, config: GatewayConfig) -> None:
        self._config = config
        self._index = RuleIndex(config.rules)

    @property
    def index(self) -> RuleIndex:
        return self._index

    def route(self, feature: RequestFeature) -> RouteDecision:
        outcome: MatchOutcome = self._index.match(feature)
        cluster = self._config.cluster(outcome.rule.cluster)
        # 规则引用了不存在的集群属于配置错误：显式失败，不静默降级
        if cluster is None:
            raise LookupError(
                f"rule '{outcome.rule.rule_id}' references unknown cluster "
                f"'{outcome.rule.cluster}'"
            )
        return RouteDecision(
            feature=feature,
            rule=outcome.rule,
            cluster=cluster,
            all_hit_rule_ids=outcome.all_hit_rule_ids,
        )
