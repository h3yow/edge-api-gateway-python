"""真实网络入口下的并发语义与三类准入边界。

与 test_network_* 中"在进程内直接调 Gateway.handle"不同，这里的请求全部
经过 FastAPI 网络入口（httpx ASGITransport，真实 asyncio 事件循环），
上游仍然是独立的本地 HTTP 进程（真实 TCP），所以能检验：

* 同一时刻进来的多个慢请求被真正并行处理（总耗时 ≈ 单个慢请求，而非逐个
  累加）；
* 某个上游变慢/卡住时，事件循环不被占住，其它请求与网关自身的
  /healthz、/__admin/state 仍然即时响应；
* rate / inflight / quota 三类准入在真实网络入口按各自边界拒绝，被拒
  请求不消耗上游执行，且并发期间绝不超额放行。

每个用例使用独立的调用方 ID，互不污染准入窗口。
"""

from __future__ import annotations

import asyncio
import socket
import subprocess
import sys
import time

import httpx
import pytest

from edge_gateway.app import create_network_app
from edge_gateway.realupstream import HttpUpstreamRegistry
from edge_gateway.upstream import UpstreamError

SLOW_DELAY = 0.5
# 并行上界 vs 串行测速：两个 0.5s 的请求并行必须远低于 0.75s，而串行会是
# 1.0s。留出调度余量但保留明显区分度。
PARALLEL_MAX = 0.85
# 上游请求超时需要大于 slow delay（failover 路径才不会误判）。
REQUEST_TIMEOUT = 3.0
PROBE_TIMEOUT = 1.0


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def _log_case(title: str, **fields) -> None:
    lines = [f"CASE {title}"]
    lines += [f"  {k}: {v}" for k, v in fields.items()]
    block = "\n".join(lines)
    print(f"\n{block}")


@pytest.fixture()
def net():
    """两个真实 users-v1 上游进程 + 网络模式网关（经 ASGI 入口调用）。"""

    instances = ["users-v1-1", "users-v1-2"]
    ports = {iid: _free_port() for iid in instances}
    procs = {
        iid: subprocess.Popen(
            [sys.executable, "-m", "edge_gateway.realupstream",
             "--id", iid, "--port", str(ports[iid])],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        for iid in instances
    }
    base_urls = {iid: f"http://127.0.0.1:{ports[iid]}" for iid in instances}

    def _kill_all() -> None:
        for proc in procs.values():
            proc.terminate()
        for proc in procs.values():
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()

    try:
        admin = HttpUpstreamRegistry(
            request_timeout=REQUEST_TIMEOUT, probe_timeout=PROBE_TIMEOUT
        )
        for iid in instances:
            admin.register(iid, base_urls[iid], "users-v1")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                for iid in instances:
                    admin.probe(iid)
                break
            except UpstreamError:
                time.sleep(0.05)
        else:
            raise AssertionError("upstreams never came up")

        # No background recovery loop: health stays deterministic and the
        # slow mode is never "healed" by a probe (slow answers /__health 200
        # anyway).
        app = create_network_app(
            base_urls=base_urls,
            start_recovery=False,
            clusters={"users-v1": instances},
        )
        env = {"app": app, "admin": admin, "instances": instances,
               "base_urls": base_urls}

        # All tests in this file drive their own event loop synchronously.
        yield env
    finally:
        _kill_all()


def _run(coro_factory):
    """Run an async factory on a fresh event loop (no pytest-asyncio dep)."""

    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro_factory())
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def _client(env) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=env["app"]),
        base_url="http://edge-gateway",
    )


# --------------------------------------------------------------------------
# 业务一：真实网络入口的并发慢请求
# --------------------------------------------------------------------------


def test_two_concurrent_slow_requests_run_in_parallel(net):
    """同一时刻打到同一个慢上游的两个请求，总耗时应接近单个慢请求。"""

    net["admin"].set_remote_mode("users-v1-1", "slow", delay=SLOW_DELAY)

    async def scenario():
        async with _client(net) as c:
            t0 = time.monotonic()
            r1, r2 = await asyncio.gather(
                c.get("/users/1", headers={"x-caller-id": "conc-a"}),
                c.get("/users/2", headers={"x-caller-id": "conc-b"}),
            )
            return time.monotonic() - t0, r1, r2

    elapsed, r1, r2 = _run(scenario)
    calls_1 = net["admin"].remote_state("users-v1-1")["calls"]
    _log_case(
        "并发业务-并发慢请求总耗时",
        two_slow_requests_each=f"{SLOW_DELAY:.2f}s",
        total_elapsed=f"{elapsed:.3f}s",
        parallel_must_be_lt=f"{PARALLEL_MAX:.2f}s",
        serial_would_be=f"{2 * SLOW_DELAY:.2f}s",
        statuses=[r1.status_code, r2.status_code],
        upstream_executions=calls_1,
    )
    assert r1.status_code == r2.status_code == 200
    assert calls_1 == 2, "两个请求都应真正打到慢上游"
    assert elapsed < PARALLEL_MAX, (
        f"两个 {SLOW_DELAY}s 请求并行总耗时 {elapsed:.2f}s，疑似被串行化"
    )


# --------------------------------------------------------------------------
# 业务二：某个上游变慢/卡住时，其它请求与网关自身接口不受影响
# --------------------------------------------------------------------------


def test_slow_upstream_does_not_freeze_gateway_endpoints(net):
    """慢请求在途期间，/healthz、/__admin/state 必须立即返回。

    关键度量：healthz 与慢请求在同一时刻发出，比较两者的墙钟完成时刻。
    若事件循环被阻塞调用占死，healthz 的完成时刻会被推迟到慢请求结束。
    """

    net["admin"].set_remote_mode("users-v1-1", "slow", delay=SLOW_DELAY)

    async def scenario():
        async with _client(net) as c:
            t0 = time.monotonic()
            done_at: dict[str, float] = {}

            async def marked(name, coro):
                try:
                    return await coro
                finally:
                    done_at[name] = time.monotonic() - t0

            # 三者在同一时刻提交；各自的完成墙钟独立记录。
            slow_task = asyncio.create_task(
                marked("slow", c.get("/users/1", headers={"x-caller-id": "freeze-slow"}))
            )
            health_task = asyncio.create_task(marked("health", c.get("/healthz")))
            state_task = asyncio.create_task(
                marked("state", c.get("/__admin/state"))
            )
            slow_resp = await slow_task
            health_resp = await health_task
            state_resp = await state_task
            return slow_resp, health_resp, state_resp, done_at

    slow_resp, health_resp, state_resp, done_at = _run(scenario)
    _log_case(
        "隔离业务-慢上游期间网关自身接口不被冻结",
        slow_upstream=f"users-v1-1 delay={SLOW_DELAY}s",
        slow_request_status=slow_resp.status_code,
        slow_completed_at=f"{done_at['slow']:.3f}s",
        healthz_status=health_resp.status_code,
        healthz_completed_at=f"{done_at['health']:.3f}s",
        healthz_must_finish_before=f"{SLOW_DELAY * 0.6:.2f}s",
        state_status=state_resp.status_code,
        state_completed_at=f"{done_at['state']:.3f}s",
        state_has_health="health" in state_resp.json(),
    )
    assert slow_resp.status_code == 200
    assert health_resp.status_code == 200
    assert health_resp.json()["ok"] is True
    assert state_resp.status_code == 200
    assert "health" in state_resp.json()
    # 两者同时刻发出：不被阻塞时 healthz 在数毫秒内完成，远早于 0.5s 的
    # 慢请求；若事件循环被阻塞调用占死，它的完成时刻会贴近慢请求。
    assert done_at["health"] < SLOW_DELAY * 0.6, (
        f"healthz 在慢请求在途时于 {done_at['health']:.2f}s 才完成，"
        "事件循环疑似被阻塞"
    )
    assert done_at["state"] < SLOW_DELAY * 0.6


def test_slow_upstream_does_not_block_unrelated_requests(net):
    """一个上游变慢时，打到另一个健康上游/其它路径的请求仍然及时。"""

    net["admin"].set_remote_mode("users-v1-1", "slow", delay=SLOW_DELAY)

    async def scenario():
        async with _client(net) as c:
            t0 = time.monotonic()
            # 慢请求（确定性首选 users-v1-1）与多次"不相干地址"请求同时刻
            # 发出：既有走另一集群实例的路径，也有网关本地直接决定的
            # NO_ROUTE 请求（根本不会访问上游）。
            slow_task = asyncio.create_task(
                c.get("/users/1", headers={"x-caller-id": "freeze-slow-2"})
            )
            other_tasks = [
                asyncio.create_task(
                    c.get(f"/nope/{i}", headers={"x-caller-id": "freeze-other"})
                )
                for i in range(3)
            ]
            times = []
            for t in asyncio.as_completed(other_tasks):
                r = await t
                times.append((time.monotonic() - t0, r.status_code))
            slow_resp = await slow_task
            return slow_resp, times, time.monotonic() - t0

    slow_resp, other_times, slow_elapsed = _run(scenario)
    _log_case(
        "隔离业务-慢上游期间不相干请求照常响应",
        unrelated_statuses=[s for _, s in other_times],
        unrelated_completed_at=[f"{t:.3f}s" for t, _ in other_times],
        slow_request_elapsed=f"{slow_elapsed:.3f}s",
        unrelated_must_finish_before=f"{SLOW_DELAY * 0.6:.2f}s",
    )
    assert slow_resp.status_code == 200
    assert len(other_times) == 3
    assert all(status == 404 for _, status in other_times)
    assert all(done < SLOW_DELAY * 0.6 for done, _ in other_times), (
        f"不相干请求完成时刻 {other_times} 被慢请求拖住"
    )


# --------------------------------------------------------------------------
# 业务三：三类准入（rate / inflight / quota）在真实网络入口的边界
# --------------------------------------------------------------------------
#
# /payments 规则：rate=3/1s，inflight=2，quota=5/3s。
# 边界结论（同一调用方、同一窗口、同时刻并发）：
#   - 恰好 3 个请求通过 rate 判定；
#   - 其中恰好 2 个拿到 inflight 槽进入上游，第 3 个被 inflight 拒绝；
#   - 之后所有请求均被拒绝，上游实际执行次数恰好 == 被允许进入的数量；
#   - 慢上游在途期间绝不超额。


def _post(c: httpx.AsyncClient, caller: str, n: int, path: str = "/payments/1"):
    headers = {"x-caller-id": caller}
    return [c.get(path, headers=headers) for _ in range(n)]


def test_rate_boundary_rejects_burst_beyond_limit_without_overadmit(net):
    """短时间冲得太猛：burst 窗口内最多 rate=3 个请求起步进入上游。

    分两波冲击、但都落在同一个 1s burst 窗口内：
      波 1（t=0）：2 个并发，恰好占满 inflight=2，0.15s 后完成并释放槽位；
      波 2（t=0.25s，同一 burst 窗口）：再冲 4 个——此时 inflight 空闲、
      burst 预算只剩 1，故只有 1 个起步，其余 3 个被 RATE 拒绝。
    整个窗口内上游真实执行次数恰好 == rate_limit=3。
    """

    burst_delay = 0.15
    net["admin"].set_remote_mode("users-v1-1", "slow", delay=burst_delay)
    headers = {"x-caller-id": "rate-boundary"}

    async def scenario():
        async with _client(net) as c:
            before = net["admin"].remote_state("users-v1-1")["calls"]
            t0 = time.monotonic()
            wave1 = await asyncio.gather(
                *[c.get("/payments/1", headers=headers) for _ in range(2)]
            )
            await asyncio.sleep(0.25 - burst_delay)  # 波 2 落在 t≈0.25s
            wave2 = await asyncio.gather(
                *[c.get("/payments/1", headers=headers) for _ in range(4)]
            )
            elapsed = time.monotonic() - t0
            after = net["admin"].remote_state("users-v1-1")["calls"]
            return wave1, wave2, elapsed, after - before

    wave1, wave2, elapsed, executions = _run(scenario)
    w1, w2 = [r.status_code for r in wave1], [r.status_code for r in wave2]
    w2_categories = [
        r.json().get("error", {}).get("details", {}).get("rejected_category")
        for r in wave2 if r.status_code != 200
    ]
    rate_rej = w2_categories.count("rate")
    _log_case(
        "准入业务-rate 短时突发边界（真实网络入口）",
        upstream_delay=f"{burst_delay}s/请求",
        wave1_statuses=w1,
        wave2_statuses=w2,
        wave2_rejected_categories=w2_categories,
        rate_rejected=rate_rej,
        upstream_executions=executions,
        elapsed=f"{elapsed:.3f}s (rate window 1.0s，两波同窗口)",
    )
    assert elapsed < 1.0, "两波必须在同一个 burst 窗口内"
    assert w1 == [200, 200]
    # 波 2：burst 只剩 1 个起步名额 -> 1 个放行，其余被 rate 拒绝。
    assert w2.count(200) == 1
    assert rate_rej == 3
    # 整个 burst 窗口恰好 3 次真实上游执行：被拒请求不占上游，绝不超额。
    assert executions == 3, (
        f"上游被执行 {executions} 次，rate 边界 3 被突破（被拒请求占用了上游）"
    )
    assert all(s in (200, 429) for s in w2)


def test_inflight_boundary_holds_under_concurrent_slow_upstream(net):
    """同时在途太多：慢上游在途时，inflight=2 之外的请求必须被拒，
    且任何时刻上游内最多 2 个该调用方请求在执行，总耗时接近单个慢请求。"""

    net["admin"].set_remote_mode("users-v1-1", "slow", delay=SLOW_DELAY)

    async def scenario():
        async with _client(net) as c:
            before = net["admin"].remote_state("users-v1-1")["calls"]
            t0 = time.monotonic()
            resps = await asyncio.gather(*_post(c, "inflight-boundary", 5))
            elapsed = time.monotonic() - t0
            after = net["admin"].remote_state("users-v1-1")["calls"]
            return resps, elapsed, after - before

    resps, elapsed, executions = _run(scenario)
    codes = sorted(r.status_code for r in resps)
    bodies = [r.json() for r in resps]
    categories = [
        b.get("error", {}).get("details", {}).get("rejected_category")
        for b in bodies if b.get("error")
    ]
    inflight_rej = categories.count("inflight")
    rate_rej = categories.count("rate")
    _log_case(
        "准入业务-inflight 同时在途边界（真实网络入口）",
        concurrent=5,
        inflight_limit=2,
        rate_limit=3,
        statuses=codes,
        rejected_categories=categories,
        inflight_rejected=inflight_rej,
        rate_rejected=rate_rej,
        upstream_executions=executions,
        total_elapsed=f"{elapsed:.3f}s (单个慢请求 {SLOW_DELAY:.2f}s)",
    )
    # 真正并发：两个被放行的请求同时在慢上游里执行，总耗时≈单个慢请求，
    # 而不是 2*delay（串行）也不是 3*delay（让 rate 放进第 3 个）。
    assert elapsed < PARALLEL_MAX, (
        f"总耗时 {elapsed:.2f}s：要么被串行化，要么超额放行了请求"
    )
    # 恰好 2 个请求进入上游并执行；其余 3 个在准入阶段被拒（inflight 或
    # rate，但绝不能是第 3 个请求真的进入了上游）。
    assert executions == 2, (
        f"慢上游实际执行 {executions} 次，inflight=2 的边界被突破"
    )
    assert sum(1 for s in codes if s == 200) == 2
    # inflight 限制必须在真实网络入口真实出现（修复前永远碰不到）。
    assert inflight_rej >= 1, (
        f"没有请求被 inflight 拒绝（拒绝类别={categories}），"
        "真实网络入口下 inflight 限制未生效"
    )
    # 被拒请求全部带明确类别，且没有任何 5xx（不该走到"无可用实例"）。
    assert all(s in (200, 429) for s in codes)


def test_quota_boundary_rejects_after_window_budget_exhausted(net):
    """一个周期内额度用光：耗尽 5 个 quota 单位后，第 6 个在新 burst 窗口
    内的请求仍被 QUOTA 拒绝，且不碰上游。

    窗口链（从首个请求起滚动）：quota 窗口 [0,3)，burst 窗口长 1s，
    inflight=2、上游 0.15s 慢响应：
      t=0.00 波 1：2 个并发占满 inflight           -> quota 2
      t=0.25 波 2：前 2 个已完成释放槽，1 个起步   -> quota 3（仍在 burst 1）
      t=1.05 波 3：burst 窗口已滚，2 个并发        -> quota 5
      t=1.30 第 6 个：又一个新 burst 窗口，rate 有预算、inflight 空闲，
                      但 quota=5 在 [0,3) 内已耗尽 -> QUOTA 拒绝，不执行上游
    """

    burst_delay = 0.15
    net["admin"].set_remote_mode("users-v1-1", "slow", delay=burst_delay)
    headers = {"x-caller-id": "quota-boundary"}

    async def scenario():
        async with _client(net) as c:
            before = net["admin"].remote_state("users-v1-1")["calls"]
            t0 = time.monotonic()
            accepted = []

            async def wave(n):
                resps = await asyncio.gather(
                    *[c.get("/payments/1", headers=headers) for _ in range(n)]
                )
                accepted.extend(resps)

            await wave(2)
            await asyncio.sleep(0.25 - burst_delay)
            await wave(1)
            await asyncio.sleep(1.05 - (0.25 - burst_delay) - burst_delay)
            await wave(2)
            assert time.monotonic() - t0 < 3.0, "第 6 个请求必须在 quota 窗口内"
            used_codes = [r.status_code for r in accepted]

            # 等到再进入一个新的 burst 窗口（rate 预算刷新），但仍处于
            # 同一个 3s quota 窗口内。
            await asyncio.sleep(0.25)
            rej = await c.get("/payments/1", headers=headers)
            assert time.monotonic() - t0 < 3.0
            after = net["admin"].remote_state("users-v1-1")["calls"]
            return used_codes, rej, after - before

    used_codes, rej, executions = _run(scenario)
    rej_body = rej.json()
    details = rej_body.get("error", {}).get("details", {})
    _log_case(
        "准入业务-quota 周期总额度边界（真实网络入口）",
        upstream_delay=f"{burst_delay}s/请求",
        accepted_statuses=used_codes,
        sixth_request_status=rej.status_code,
        sixth_request_error=rej_body.get("error", {}).get("code"),
        rejected_category=details.get("rejected_category"),
        quota_snapshot=details.get("quota"),
        rate_snapshot=details.get("rate"),
        upstream_executions=executions,
    )
    assert used_codes == [200, 200, 200, 200, 200]
    assert rej.status_code == 429
    assert rej_body["error"]["code"] == "QUOTA_EXCEEDED"
    assert details["rejected_category"] == "quota"
    assert details["quota"]["used"] == 5
    assert details["quota"]["remaining"] == 0
    # 此时 rate 窗口已刷新（证明放行它过得了 rate），是 quota 单独拒绝。
    assert details["rate"]["remaining"] is not None and details["rate"]["remaining"] >= 1
    # 被 quota 拒绝的第 6 个请求不得消耗上游：恰好 5 次真实执行。
    assert executions == 5, (
        f"quota 拒绝后上游仍被执行（总执行 {executions} 次），拒绝未做到"
        "不占上游资源"
    )
