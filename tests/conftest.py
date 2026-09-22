"""pytest 共享夹具与判定日志输出。

关键设计：所有“并发 + 上游抖动”测试都用**可控假时钟**驱动网关内部的
限流与健康状态机，从而让“限流边界、退避恢复时长”等结论与机器速度
无关、稳定可复现；真实的线程并发仍然保留（线程池 + 真实锁）。
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import httpx
import pytest

from gateway.config import HealthSettings, demo_config
from gateway.models import RequestFeature
from gateway.proxy import Gateway
from sim.local_run import SimCluster


@dataclass
class Clock:
    """手动推进的单调时钟。"""

    t: float = 1000.0

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@dataclass
class ProbeRouter:
    """把实例健康检查路由到对应模拟实例（真实 HTTP）。

    实例被停止（连接拒绝）时返回 False，不抛异常。
    """

    cluster: SimCluster

    def __call__(self, instance) -> bool:
        handle = self.cluster.get(instance.instance_id)
        if handle._handle is None:
            return False  # 实例已停止
        try:
            response = httpx.get(instance.base_url.rstrip("/") + "/healthz",
                                 timeout=1.0)
            ok = 200 <= response.status_code < 400
            response.close()
            return ok
        except httpx.HTTPError:
            return False


@pytest.fixture
def cluster():
    sim = SimCluster()
    sim.start_all()
    yield sim
    sim.stop_all()


@pytest.fixture
def gateway_factory(cluster):
    """构造使用假时钟的网关；每个测试独立创建，互不污染状态。"""

    created: list[Gateway] = []

    def _factory(
        health: HealthSettings | None = None,
        timeout: float = 3.0,
        clock: Clock | None = None,
        enable_real_forwarding: bool = True,
    ) -> tuple[Gateway, Clock]:
        clock = clock or Clock()
        settings = health or HealthSettings(
            failure_threshold=3,
            initial_cooldown_seconds=0.5,
            max_cooldown_seconds=8.0,
        )
        probe_client = httpx.Client(timeout=1.0)
        gw = Gateway(
            cluster.config,
            settings,
            time_func=clock,
            probe_client=probe_client,
        )
        # 用“经过实例管理器”的探测替换裸探测（支持停机模拟）
        gw._probe_client = probe_client  # noqa: SLF001
        gw.forwarder._probe = ProbeRouter(cluster)  # noqa: SLF001
        created.append(gw)
        return gw, clock

    yield _factory

    for gw in created:
        gw.close()


@pytest.fixture
def gateway(gateway_factory):
    gw, clock = gateway_factory()
    return gw, clock


def concurrent_call(fn, count: int, workers: int | None = None) -> list:
    """用线程池并发执行 ``fn(index)``，返回按完成顺序收集的结果。

    使用起始屏障让线程尽量同时发起；workers 默认等于任务数，
    避免“屏障占用线程池槽位导致饥饿死锁”。
    """
    workers = workers or count
    barrier = threading.Barrier(count)

    def _wrap(i: int):
        barrier.wait()  # 尽量让所有线程同时发起，制造真实竞争
        return fn(i)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(_wrap, range(count)))


def feature(method: str = "GET", path: str = "/api/orders",
            caller: str = "demo-team", **headers) -> RequestFeature:
    headers = {f"X-{k.replace('_', '-').title()}": v for k, v in headers.items()}
    headers["X-Caller-Id"] = caller
    return RequestFeature(method=method, path=path, headers=headers)
