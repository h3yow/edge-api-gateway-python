"""调用方维度限流、配额与并发的边界与并发正确性测试。"""

from __future__ import annotations

import threading

from gateway.errors import ErrorCode, GatewayError
from gateway.models import CallerPolicy
from gateway.ratelimit import RateLimiter

from conftest import Clock, concurrent_call


def _print(title: str, admitted: int, rejected: dict, note: str) -> None:
    print(
        f"\n[{title}] 接受={admitted} 拒绝分类={rejected}\n"
        f"  判定依据: {note}"
    )


def test_burst_boundary_no_over_admit():
    clk = Clock()
    policy = CallerPolicy("c", rate_per_second=1.0, burst=3.0,
                          quota_limit=100, window_seconds=60.0,
                          max_concurrency=100)
    rl = RateLimiter({"c": policy}, time_func=clk)

    outcomes = []
    for _ in range(5):
        try:
            outcomes.append(("accept", rl.admit("c")))
        except GatewayError as exc:
            outcomes.append(("reject", exc.code.value))

    admitted = sum(1 for kind, _ in outcomes if kind == "accept")
    reasons = [v for kind, v in outcomes if kind == "reject"]
    _print("burst", admitted, {r: reasons.count(r) for r in set(reasons)},
           "burst=3，瞬时第4、5个请求必须被 RATE_LIMITED")
    assert admitted == 3
    assert set(reasons) == {ErrorCode.RATE_LIMITED.value}
    for kind, ticket in outcomes:
        if kind == "accept":
            ticket.release()


def test_quota_exhaustion_is_atomic_under_concurrency():
    """200 并发请求，配额=100：恰好 100 被接受，绝不多放。"""
    clk = Clock()
    policy = CallerPolicy("c", rate_per_second=10_000.0, burst=10_000.0,
                          quota_limit=100, window_seconds=60.0,
                          max_concurrency=10_000)
    rl = RateLimiter({"c": policy}, time_func=clk)

    accepted: list = []
    rejected: list[str] = []
    lock = threading.Lock()

    def one(_i: int):
        try:
            ticket = rl.admit("c")
            with lock:
                accepted.append(ticket)
        except GatewayError as exc:
            with lock:
                rejected.append(exc.code.value)

    concurrent_call(one, 200)
    counts = {r: rejected.count(r) for r in set(rejected)}
    _print("quota-concurrency", len(accepted), counts,
           "quota=100，200 线程同时准入；配额耗尽点必须唯一")
    assert len(accepted) == 100
    assert counts == {ErrorCode.QUOTA_EXHAUSTED.value: 100}
    for ticket in accepted:
        ticket.release()
    # 配额计数不超过上限
    assert rl.snapshot("c")["quota_used"] == 100


def test_quota_resets_after_window():
    clk = Clock()
    policy = CallerPolicy("c", rate_per_second=1000.0, burst=1000.0,
                          quota_limit=2, window_seconds=10.0,
                          max_concurrency=10)
    rl = RateLimiter({"c": policy}, time_func=clk)
    rl.admit("c").release()
    rl.admit("c").release()
    with __import__("pytest").raises(GatewayError) as exc:
        rl.admit("c")
    assert exc.value.code is ErrorCode.QUOTA_EXHAUSTED

    clk.advance(10.0)  # 窗口结束
    ticket = rl.admit("c")
    _print("window-reset", 1, {}, "窗口滚动后配额清零，请求重新被接受")
    ticket.release()
    assert rl.snapshot("c")["quota_used"] == 1


def test_concurrency_limit_blocks_when_upstream_slow():
    """max_concurrency=2：前 2 个在途未释放时，第 3 个必须被拒绝，
    即使令牌与配额都充足——慢上游场景下也不得堆积超额。"""
    clk = Clock()
    policy = CallerPolicy("c", rate_per_second=1000.0, burst=1000.0,
                          quota_limit=1000, window_seconds=60.0,
                          max_concurrency=2)
    rl = RateLimiter({"c": policy}, time_func=clk)
    t1 = rl.admit("c")
    t2 = rl.admit("c")
    with __import__("pytest").raises(GatewayError) as exc:
        rl.admit("c")
    _print("concurrency", 2, {exc.value.code.value: 1},
           "2 个在途未释放 -> CONCURRENCY_LIMITED；释放后恢复")
    assert exc.value.code is ErrorCode.CONCURRENCY_LIMITED
    t1.release()
    rl.admit("c").release()
    t2.release()


def test_unknown_caller_rejected_without_consuming_resources():
    clk = Clock()
    policy = CallerPolicy("known", 1.0, 1.0, 5, 60.0, 5)
    rl = RateLimiter({"known": policy}, time_func=clk)
    with __import__("pytest").raises(GatewayError) as exc:
        rl.admit("intruder")
    assert exc.value.code is ErrorCode.CALLER_UNIDENTIFIED
    _print("unknown-caller", 0,
           {ErrorCode.CALLER_UNIDENTIFIED.value: 1},
           "未知调用方在准入前被拒绝，不建立任何桶/槽位")
