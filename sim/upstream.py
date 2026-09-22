"""本地模拟上游服务。

每个实例是一个独立的 FastAPI 应用，支持运行时控制：

* ``POST /_sim/set_healthy``   body ``{"healthy": false}``：关闭/开启健康检查；
* ``POST /_sim/set_behaviour`` body ``{"mode": "http_500"|"reset"|"slow",
  "seconds": 1.0}``：控制业务端口的故障表现；
  - ``http_500``：所有业务请求返回 500；
  - ``reset``：直接关闭连接（模拟进程崩溃/拒绝服务）；
  - ``slow``：延迟若干秒再响应（配合超时与并发测试）；
  - ``ok``：恢复正常；
* ``GET /_sim/state``：查看当前状态与**按方法统计的请求执行次数**。

“执行计数”用于在测试中断言：非幂等 POST 在故障转移过程中
**只可能被执行一次**。
"""

from __future__ import annotations

import threading
import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response


class SimState:
    def __init__(self, instance_id: str, cluster: str) -> None:
        self.instance_id = instance_id
        self.cluster = cluster
        self.healthy = True
        self.mode = "ok"
        self.slow_seconds = 0.0
        self.method_counts: dict[str, int] = {}
        self.total = 0
        self._lock = threading.Lock()

    def count(self, method: str) -> None:
        with self._lock:
            self.method_counts[method] = self.method_counts.get(method, 0) + 1
            self.total += 1

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "instance_id": self.instance_id,
                "cluster": self.cluster,
                "healthy": self.healthy,
                "mode": self.mode,
                "slow_seconds": self.slow_seconds,
                "method_counts": dict(self.method_counts),
                "total": self.total,
            }


def create_upstream(instance_id: str, cluster: str) -> tuple[FastAPI, SimState]:
    state = SimState(instance_id, cluster)
    app = FastAPI(title=f"sim-upstream-{instance_id}")

    @app.get("/healthz")
    def healthz() -> Response:
        # 健康检查路径本身不计数，也不受业务故障模式影响
        if state.healthy:
            return JSONResponse({"ok": True, "instance": instance_id})
        return JSONResponse(
            {"ok": False, "instance": instance_id}, status_code=503
        )

    @app.post("/_sim/set_healthy")
    async def set_healthy(request: Request) -> dict:
        body = await request.json()
        state.healthy = bool(body.get("healthy", True))
        return state.snapshot()

    @app.post("/_sim/set_behaviour")
    async def set_behaviour(request: Request) -> dict:
        body = await request.json()
        state.mode = str(body.get("mode", "ok"))
        state.slow_seconds = float(body.get("seconds", 0.0))
        return state.snapshot()

    @app.get("/_sim/state")
    def get_state() -> dict:
        return state.snapshot()

    @app.api_route(
        "/{full_path:path}",
        methods=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"],
        include_in_schema=False,
    )
    async def business(full_path: str, request: Request) -> Response:
        state.count(request.method)

        if state.mode == "slow" and state.slow_seconds > 0:
            time.sleep(state.slow_seconds)
        if state.mode == "http_500":
            return JSONResponse(
                {"upstream_error": "simulated-500", "instance": instance_id},
                status_code=500,
            )
        if state.mode == "reset":
            # 传输层故障（连接拒绝/重置）在真实环境里对应“进程不可达”，
            # 本地由 sim.local_run 的服务器管理器通过停止实例来模拟
            # （端口直接拒绝连接），比在应用内伪造更真实；
            # 若仅切换了模式而没有停服，返回明确的 502 以便区分。
            return Response(status_code=502, content=b"simulated reset")

        body = await request.body()
        return JSONResponse(
            {
                "served_by": instance_id,
                "cluster": cluster,
                "method": request.method,
                "path": "/" + full_path,
                "received_bytes": len(body),
                "request_headers_echo": {
                    k: v
                    for k, v in request.headers.items()
                    if k.lower() in ("x-caller-id", "x-tenant")
                },
            }
        )

    return app, state
