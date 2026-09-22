"""健康状态机的确定性单测：阈值摘除、退避、单飞探测、防抖动。"""

from __future__ import annotations

import threading

from gateway.health import HealthState, InstanceHealth
from gateway.models import UpstreamInstance

from conftest import Clock, concurrent_call


def _monitor(clock: Clock, threshold: int = 3):
    return InstanceHealth(
        UpstreamInstance("i1", "http://127.0.0.1:1"),
        failure_threshold=threshold,
        initial_cooldown=1.0,
        max_cooldown=8.0,
        time_func=clock,
    )


def test_suspend_only_once_at_threshold():
    clk = Clock()
    mon = _monitor(clk, threshold=3)
    events = []
    for _ in range(10):
        events += mon.report_failure()
    suspends = [e for e in events if e.transition == "suspend"]
    print("\n[threshold] 10 次连续失败 -> 摘除事件数:", len(suspends),
          "状态:", mon.snapshot()["state"])
    assert len(suspends) == 1
    assert mon.snapshot()["state"] == HealthState.SUSPENDED.value


def test_failed_probe_extends_cooldown_without_state_flap():
    clk = Clock()
    mon = _monitor(clk)
    for _ in range(3):
        mon.report_failure()

    clk.advance(1.0)
    mon.maybe_probe(lambda ins: False)  # 探测失败
    snap = mon.snapshot()
    print("\n[failed-probe] 状态保持:", snap["state"],
          "下次探测时刻:", snap["next_probe_at"])
    assert snap["state"] == HealthState.SUSPENDED.value
    # 基准 1000：摘除@1000，首轮退避1s；探测失败于1001，退避翻倍为2s
    assert snap["next_probe_at"] == pytest_approx(1003.0)

    clk.advance(2.0)
    mon.maybe_probe(lambda ins: False)
    snap2 = mon.snapshot()
    assert snap2["state"] == HealthState.SUSPENDED.value
    assert snap2["next_probe_at"] == pytest_approx(1007.0)  # 1003 + 2^2


def pytest_approx(expected: float):
    return round(expected, 6)


def test_single_flight_probe_under_concurrency():
    """20 线程同时在探测到期点触发：只允许发起一次探测、产生一次恢复。"""
    clk = Clock()
    mon = _monitor(clk)
    for _ in range(3):
        mon.report_failure()
    clk.advance(1.0)

    started = threading.Lock()
    probe_calls: list[int] = []

    def probe(ins):
        with started:
            probe_calls.append(1)
        return True

    event_lists = concurrent_call(lambda i: mon.maybe_probe(probe), 20)
    total_events = [e for evs in event_lists for e in evs]
    snap = mon.snapshot()
    print("\n[single-flight] 20 并发 -> 实际探测次数:", len(probe_calls),
          "恢复事件数(各线程可见):", len(total_events),
          "累计探测:", snap["probes_started"], "状态:", snap["state"])
    assert len(probe_calls) == 1
    assert len(total_events) == 1
    assert snap["state"] == HealthState.HEALTHY.value
    assert snap["probes_started"] == 1


def test_recover_resets_failure_count():
    clk = Clock()
    mon = _monitor(clk)
    for _ in range(3):
        mon.report_failure()
    clk.advance(1.0)
    mon.maybe_probe(lambda ins: True)
    snap = mon.snapshot()
    print("\n[recover-reset] 恢复后失败计数:", snap["consecutive_failures"])
    assert snap["consecutive_failures"] == 0
