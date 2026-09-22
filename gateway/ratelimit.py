"""调用方维度的限流、配额与并发控制。

三层控制，语义彼此独立、拒绝原因可区分：

1. **速率（令牌桶）**：按固定速率补充令牌，桶容量为 burst；
   并发请求各自预占 1 个令牌，令牌不足立即以 ``RATE_LIMITED`` 拒绝；
2. **配额（固定窗口）**：每个窗口最多放行 quota_limit 个请求，窗口结束
   自动重置；耗尽以 ``QUOTA_EXHAUSTED`` 拒绝，并返回窗口重置时刻；
3. **并发（在途槽位）``：同一调用方同时在途请求不得超过 max_concurrency，
   防止“上游变慢导致请求堆积超额”；以 ``CONCURRENCY_LIMITED`` 拒绝。

关键并发保证：三层检查在**每调用方一把锁**内顺序完成并同时预占，
外部观察到“配额尚未耗尽的请求一定被接受，耗尽之后一定被拒绝”，
不会出现两个并发请求同时看到最后一个配额而双发放行。
被拒绝的请求在锁内即返回，不接触任何上游资源。
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable, Protocol

from gateway.errors import (
    GatewayError,
    caller_unidentified,
    concurrency_limited,
    quota_exhausted,
    rate_limited,
)
from gateway.models import CallerPolicy

TimeFunc = Callable[[], float]


class Clock(Protocol):
    def __call__(self) -> float: ...


@dataclass
class _Bucket:
    tokens: float
    last_refill: float
    window_start: float
    quota_used: int
    in_flight: int


class AdmissionTicket:
    """准入凭证：请求结束时必须恰好释放一次。"""

    def __init__(self, caller: str, limiter: "RateLimiter") -> None:
        self._caller = caller
        self._limiter = limiter
        self._released = False

    @property
    def caller_id(self) -> str:
        return self._caller

    def release(self) -> None:
        if self._released:  # 双重释放属于编程错误，立即暴露
            raise RuntimeError("admission ticket released twice")
        self._released = True
        self._limiter._release(self._caller)


class RateLimiter:
    def __init__(
        self,
        policies: dict[str, CallerPolicy],
        *,
        time_func: TimeFunc = time.monotonic,
    ) -> None:
        self._policies = dict(policies)
        self._time = time_func
        self._locks: dict[str, threading.Lock] = {
            cid: threading.Lock() for cid in policies
        }
        self._buckets: dict[str, _Bucket] = {
            cid: _Bucket(
                tokens=p.burst,
                last_refill=time_func(),
                window_start=time_func(),
                quota_used=0,
                in_flight=0,
            )
            for cid, p in policies.items()
        }

    def snapshot(self, caller: str) -> dict:
        """返回调用方当前配额视图（用于解释与调试）。"""
        with self._locks[caller]:
            self._refill(caller)
            b = self._buckets[caller]
            p = self._policies[caller]
            return {
                "caller_id": caller,
                "available_tokens": round(b.tokens, 4),
                "rate_per_second": p.rate_per_second,
                "burst": p.burst,
                "quota_used": b.quota_used,
                "quota_limit": p.quota_limit,
                "quota_remaining": p.quota_limit - b.quota_used,
                "window_reset_at": round(b.window_start + p.window_seconds, 4),
                "in_flight": b.in_flight,
                "max_concurrency": p.max_concurrency,
            }

    def admit(self, caller: str) -> AdmissionTicket:
        """原子地完成三层检查并预占资源；失败抛出可区分的 GatewayError。"""
        policy = self._policies.get(caller)
        if policy is None:
            raise caller_unidentified()
        lock = self._locks[caller]
        with lock:
            now = self._time()
            bucket = self._buckets[caller]
            self._advance(bucket, policy, now)

            # 1) 并发槽位：必须最先预占，保证慢上游不会导致在途数超限
            if bucket.in_flight >= policy.max_concurrency:
                raise concurrency_limited(
                    caller, bucket.in_flight, policy.max_concurrency
                )

            # 2) 配额
            if bucket.quota_used >= policy.quota_limit:
                raise quota_exhausted(
                    caller,
                    bucket.quota_used,
                    policy.quota_limit,
                    bucket.window_start + policy.window_seconds,
                )

            # 3) 速率令牌
            if bucket.tokens < 1.0:
                retry_after = (1.0 - bucket.tokens) / policy.rate_per_second \
                    if policy.rate_per_second > 0 else float("inf")
                raise rate_limited(caller, retry_after, "token_bucket")

            # 全部通过：一次性预占
            bucket.tokens -= 1.0
            bucket.quota_used += 1
            bucket.in_flight += 1
            return AdmissionTicket(caller, self)

    def _release(self, caller: str) -> None:
        with self._locks[caller]:
            self._buckets[caller].in_flight -= 1

    def reset_window(self, caller: str) -> None:
        """测试/管理用途：立即重置配额窗口。"""
        with self._locks[caller]:
            self._buckets[caller].quota_used = 0
            self._buckets[caller].window_start = self._time()

    def _refill(self, caller: str) -> None:
        self._advance(self._buckets[caller], self._policies[caller], self._time())

    @staticmethod
    def _advance(bucket: _Bucket, policy: CallerPolicy, now: float) -> None:
        # 令牌补充
        elapsed = max(0.0, now - bucket.last_refill)
        bucket.tokens = min(
            policy.burst, bucket.tokens + elapsed * policy.rate_per_second
        )
        bucket.last_refill = now
        # 配额窗口滚动
        if now - bucket.window_start >= policy.window_seconds:
            bucket.window_start = now
            bucket.quota_used = 0
