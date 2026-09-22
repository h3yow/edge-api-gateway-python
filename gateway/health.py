"""上游实例健康状态机与故障探测。

状态只有两种：

* ``HEALTHY``：可承载流量；
* ``SUSPENDED``：连续失败达到阈值后被暂时摘除，不参与正常选择，
  退避到期后允许一次单飞探测，探测成功恢复为 ``HEALTHY``。

并发安全保证：

* 被动失败上报与摘除在每实例锁内完成；只有“连续失败数从阈值-1 变成
  阈值”这一次跳变会产生摘除事件，重复上报不会重复摘除；
* 探测使用单飞（probe lock + 双重检查）：退避到期的同一时刻即便有
  大量并发请求触发探测，实际也只会发起**一次**探测，其余请求等待并
  观察同一结果，因此不会出现状态反复抖动；
* 探测失败不会在 HEALTHY/SUSPENDED 间来回跳变——探测失败只是安排
  下一轮更长的退避，状态始终保持 SUSPENDED。

探测动作以可调用对象注入（``probe_fn(instance) -> bool``），
本模块不直接做 HTTP，便于用假时钟做确定性测试。
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Protocol

from gateway.models import UpstreamInstance

TimeFunc = Callable[[], float]


class HealthState(str, Enum):
    HEALTHY = "HEALTHY"
    SUSPENDED = "SUSPENDED"


class ProbeFn(Protocol):
    def __call__(self, instance: UpstreamInstance) -> bool: ...


@dataclass
class HealthEvent:
    instance_id: str
    transition: str  # "suspend" | "recover"
    at: float
    consecutive_failures: int
    detail: str = ""


@dataclass
class _State:
    state: HealthState = HealthState.HEALTHY
    consecutive_failures: int = 0
    suspended_at: float | None = None
    next_probe_at: float = 0.0
    probe_round: int = 0
    probes_started: int = 0
    probes_succeeded: int = 0


class InstanceHealth:
    """单个实例的健康跟踪。"""

    def __init__(
        self,
        instance: UpstreamInstance,
        *,
        failure_threshold: int = 3,
        initial_cooldown: float = 0.5,
        max_cooldown: float = 30.0,
        time_func: TimeFunc = time.monotonic,
    ) -> None:
        self.instance = instance
        self.failure_threshold = failure_threshold
        self.initial_cooldown = initial_cooldown
        self.max_cooldown = max_cooldown
        self._time = time_func
        self._s = _State()
        self._state_lock = threading.Lock()
        self._probe_lock = threading.Lock()  # 单飞探测锁

    # ---- 只读视图 ----
    def snapshot(self) -> dict:
        with self._state_lock:
            s = self._s
            return {
                "instance_id": self.instance.instance_id,
                "state": s.state.value,
                "consecutive_failures": s.consecutive_failures,
                "suspended_at": s.suspended_at,
                "next_probe_at": s.next_probe_at,
                "probe_round": s.probe_round,
                "probes_started": s.probes_started,
                "probes_succeeded": s.probes_succeeded,
            }

    def is_healthy(self) -> bool:
        with self._state_lock:
            return self._s.state is HealthState.HEALTHY

    def due_for_probe(self, now: float | None = None) -> bool:
        now = self._time() if now is None else now
        with self._state_lock:
            return self._is_due_locked(now)

    def _is_due_locked(self, now: float) -> bool:
        """调用方必须已持有 _state_lock。"""
        return (
            self._s.state is HealthState.SUSPENDED
            and now >= self._s.next_probe_at
        )

    # ---- 被动健康上报（来自真实流量）----
    def report_success(self) -> list[HealthEvent]:
        with self._state_lock:
            self._s.consecutive_failures = 0
            return []  # 正常状态不产生恢复事件，避免抖动噪声

    def report_failure(self, detail: str = "") -> list[HealthEvent]:
        with self._state_lock:
            self._s.consecutive_failures += 1
            events: list[HealthEvent] = []
            if (
                self._s.state is HealthState.HEALTHY
                and self._s.consecutive_failures >= self.failure_threshold
            ):
                now = self._time()
                self._s.state = HealthState.SUSPENDED
                self._s.suspended_at = now
                self._s.probe_round = 0
                self._s.next_probe_at = now + self._cooldown(0)
                events.append(
                    HealthEvent(
                        self.instance.instance_id,
                        "suspend",
                        now,
                        self._s.consecutive_failures,
                        detail,
                    )
                )
            return events

    # ---- 主动探测：单飞 ----
    def maybe_probe(self, probe_fn: ProbeFn) -> list[HealthEvent]:
        """若已到探测时刻则发起至多一次探测；返回本次产生的状态事件。

        并发调用时只有一个线程真正执行探测，其余线程阻塞并复用结果，
        因此返回的事件要么对全部等待者一致（恢复事件），要么为空。
        """
        now = self._time()
        with self._state_lock:
            if not self._is_due_locked(now):
                return []
        # 单飞入口：同时刻只允许一个线程进入探测
        if not self._probe_lock.acquire(blocking=False):
            with self._probe_lock:  # 等待在途探测结束，复用其结论
                return []
        try:
            # 双重检查：可能在等待锁期间状态已被其他线程改变
            now = self._time()
            with self._state_lock:
                if self._s.state is HealthState.HEALTHY or now < self._s.next_probe_at:
                    return []
                round_ = self._s.probe_round
                self._s.probes_started += 1
            ok = bool(probe_fn(self.instance))
            return self._record_probe_result(ok, round_)
        finally:
            self._probe_lock.release()

    def _record_probe_result(self, ok: bool, round_: int) -> list[HealthEvent]:
        with self._state_lock:
            now = self._time()
            events: list[HealthEvent] = []
            if ok:
                if self._s.state is HealthState.SUSPENDED:
                    self._s.state = HealthState.HEALTHY
                    self._s.consecutive_failures = 0
                    self._s.suspended_at = None
                    self._s.probe_round = 0
                    self._s.probes_succeeded += 1
                    events.append(
                        HealthEvent(
                            self.instance.instance_id, "recover", now, 0
                        )
                    )
                else:
                    self._s.probes_succeeded += 1
            else:
                # 探测失败：保持 SUSPENDED，安排更长退避，绝不状态抖动
                self._s.probe_round = round_ + 1
                self._s.next_probe_at = now + self._cooldown(round_ + 1)
            return events

    def _cooldown(self, round_: int) -> float:
        return min(self.initial_cooldown * (2 ** round_), self.max_cooldown)


@dataclass
class HealthRegistry:
    """管理所有实例的健康跟踪，并可周期性统一探测。"""

    monitors: dict[str, InstanceHealth] = field(default_factory=dict)
    events: list[HealthEvent] = field(default_factory=list)
    _events_lock: threading.Lock = field(default_factory=threading.Lock)

    def get(self, instance_id: str) -> InstanceHealth:
        return self.monitors[instance_id]

    def record_events(self, events: list[HealthEvent]) -> None:
        if events:
            with self._events_lock:
                self.events.extend(events)

    def healthy_ids(self) -> list[str]:
        return [
            iid
            for iid, mon in self.monitors.items()
            if mon.is_healthy()
        ]

    def probe_due(self, probe_fn: ProbeFn) -> list[HealthEvent]:
        all_events: list[HealthEvent] = []
        for mon in self.monitors.values():
            events = mon.maybe_probe(probe_fn)
            if events:
                all_events.extend(events)
                self.record_events(events)
        return all_events

    def event_log(self) -> list[dict]:
        with self._events_lock:
            return [
                {
                    "instance_id": e.instance_id,
                    "transition": e.transition,
                    "at": round(e.at, 4),
                    "consecutive_failures": e.consecutive_failures,
                    "detail": e.detail,
                }
                for e in list(self.events)
            ]
