"""上游候选选择。

在一个集群内产生“确定且可解释”的尝试顺序：

1. 健康实例按 instance_id 排序构成基础顺序（任何入口、任何时刻一致）；
2. 用每集群原子轮转游标在健康集合上做起点轮换，分散流量；
   轮换是“在确定排序之上的确定移位”，不引入随机；
3. 摘除中的实例不进入候选；若所有实例都摘除了，候选为空，
   由上层返回 ``UPSTREAM_UNAVAILABLE``（之前可先触发一次单飞探测再重试）。
"""

from __future__ import annotations

import itertools
import threading

from gateway.health import HealthRegistry
from gateway.models import Cluster, UpstreamInstance


class InstancePool:
    def __init__(self, registry: HealthRegistry) -> None:
        self._registry = registry
        self._cursors: dict[str, itertools.count] = {}
        self._lock = threading.Lock()

    def healthy_instances(self, cluster: Cluster) -> list[UpstreamInstance]:
        """当前健康实例，按 instance_id 确定排序。"""
        healthy = set(self._registry.healthy_ids())
        return sorted(
            (ins for ins in cluster.instances if ins.instance_id in healthy),
            key=lambda i: i.instance_id,
        )

    def all_instances_ordered(self, cluster: Cluster) -> list[UpstreamInstance]:
        return sorted(cluster.instances, key=lambda i: i.instance_id)

    def candidates(self, cluster: Cluster) -> list[UpstreamInstance]:
        """返回本次请求的尝试顺序（健康实例的确定轮转排列）。"""
        ordered = self.healthy_instances(cluster)
        if not ordered:
            return []
        with self._lock:
            counter = self._cursors.setdefault(cluster.name, itertools.count())
            shift = next(counter) % len(ordered)
        return ordered[shift:] + ordered[:shift]

    def is_healthy(self, instance_id: str) -> bool:
        return self._registry.get(instance_id).is_healthy()
