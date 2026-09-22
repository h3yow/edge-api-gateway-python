"""规则匹配引擎。

匹配优先级是完全确定的：

1. **具体度优先**：路径分段数多者优先；相同则请求头约束多者优先；
   再相同则限定了方法的规则优先；
2. 具体度完全相同时，按 ``rule_id`` 字典序兜底，保证“同一请求在任何入口
   下都命中同一条规则”。

冲突的定义：两条以上规则同时命中、且具体度并列最高时，无法依据优先级
区分，判定为冲突（:class:`~gateway.errors.GatewayError`,
``ROUTE_AMBIGUOUS``），绝不随机或静默选择。
"""

from __future__ import annotations

from dataclasses import dataclass

from gateway.errors import ambiguous, no_match
from gateway.models import RequestFeature, RouteRule


@dataclass(frozen=True)
class MatchOutcome:
    rule: RouteRule
    all_hit_rule_ids: tuple[str, ...]
    rank: tuple[int, int, int]


class RuleIndex:
    """不可变规则索引，可被并发请求安全共享。"""

    def __init__(self, rules: tuple[RouteRule, ...] | list[RouteRule]) -> None:
        # 静态顺序：具体度降序、rule_id 升序
        self._rules: tuple[RouteRule, ...] = tuple(
            sorted(rules, key=lambda r: _priority_key(r))
        )

    @property
    def rules(self) -> tuple[RouteRule, ...]:
        return self._rules

    def all_hits(self, feature: RequestFeature) -> tuple[RouteRule, ...]:
        return tuple(r for r in self._rules if r.matches(feature))

    def match(self, feature: RequestFeature) -> MatchOutcome:
        """返回唯一命中；无命中或同优先级冲突时抛出网关错误。"""
        hits = self.all_hits(feature)
        if not hits:
            raise no_match(feature.method, feature.path, len(self._rules))

        top_rank = max(r.specificity() for r in hits)
        top_hits = sorted((r for r in hits if r.specificity() == top_rank),
                          key=lambda r: r.rule_id)

        if len(top_hits) > 1:
            # 具体度并列最高 -> 冲突；rule_id 顺序仅用于稳定地报告，绝不自动挑选
            raise ambiguous(
                feature.method,
                feature.path,
                [r.rule_id for r in top_hits],
            )

        winner = top_hits[0]
        return MatchOutcome(
            rule=winner,
            all_hit_rule_ids=tuple(r.rule_id for r in hits),
            rank=top_rank,
        )

    def explain(self, feature: RequestFeature) -> list[dict]:
        """按优先级顺序列出每条规则的匹配情况，用于判定日志。"""
        rows: list[dict] = []
        for rule in self._rules:
            rows.append(
                {
                    "rule_id": rule.rule_id,
                    "path_prefix": rule.path_prefix,
                    "methods": sorted(rule.methods),
                    "require_headers": [list(h) for h in rule.require_headers],
                    "matches": rule.matches(feature),
                    "specificity": list(rule.specificity()),
                }
            )
        return rows


def _priority_key(rule: RouteRule) -> tuple[int, int, int, str]:
    """前三段取负实现具体度降序，rule_id 自然升序作为最终兜底。"""
    a, b, c = rule.specificity()
    return (-a, -b, -c, rule.rule_id)


def sorted_rules(rules: tuple[RouteRule, ...]) -> tuple[RouteRule, ...]:
    return tuple(sorted(rules, key=_priority_key))
