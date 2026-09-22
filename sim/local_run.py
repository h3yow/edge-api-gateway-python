"""本地一键运行模拟上游集群与边缘网关。

提供两种用法：

* 脚本：``python -m sim.local_run`` 启动全部模拟上游与网关，
  网关监听 8080；
* 编程方式：:class:`SimCluster` 在测试中按需启动/停止单个实例，
  以制造“端口拒绝连接”这类真实的传输层故障。
"""

from __future__ import annotations

import socket
import threading
import time
from dataclasses import dataclass

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from gateway.config import demo_config
from sim.upstream import SimState


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class _ServerHandle:
    def __init__(self, app, host: str, port: int) -> None:
        self.config = uvicorn.Config(
            app,
            host=host,
            port=port,
            log_level="error",
            access_log=False,
        )
        self.server = uvicorn.Server(self.config)
        self.thread = threading.Thread(
            target=self.server.run, name=f"uvicorn-{port}", daemon=True
        )

    def start(self) -> None:
        self.thread.start()
        while not self.server.started:
            time.sleep(0.005)

    def stop_server(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=5)


@dataclass
class InstanceHandle:
    instance_id: str
    cluster: str
    port: int
    base_url: str
    state: SimState
    _handle: _ServerHandle | None = None
    _app: object | None = None

    def start(self) -> None:
        if self._handle is not None:
            return
        app = _rebuild_app_with_state(self.state)
        self._app = app
        handle = _ServerHandle(app, "127.0.0.1", self.port)
        handle.start()
        self._handle = handle

    def stop(self) -> None:
        if self._handle is None:
            return
        self._handle.stop_server()
        self._handle = None

    def restart(self) -> None:
        self.stop()
        time.sleep(0.05)
        self.start()


def _rebuild_app_with_state(state: SimState):
    """用既有 SimState 构造上游应用（重启实例时保留统计对象）。"""
    app = FastAPI(title=f"sim-upstream-{state.instance_id}")

    @app.get("/healthz")
    def healthz() -> Response:
        if state.healthy:
            return JSONResponse({"ok": True, "instance": state.instance_id})
        return JSONResponse(
            {"ok": False, "instance": state.instance_id}, status_code=503
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
                {"upstream_error": "simulated-500", "instance": state.instance_id},
                status_code=500,
            )
        if state.mode == "reset":
            return Response(status_code=502, content=b"simulated reset")
        body = await request.body()
        return JSONResponse(
            {
                "served_by": state.instance_id,
                "cluster": state.cluster,
                "method": request.method,
                "path": "/" + full_path,
                "received_bytes": len(body),
            }
        )

    return app


class SimCluster:
    """按 demo_config 启动全部模拟实例，并跟踪端口与状态。"""

    def __init__(self) -> None:
        self.config, self.health_settings = demo_config()
        self.port_map: dict[str, int] = {}
        self.handles: dict[str, InstanceHandle] = {}
        for cluster in self.config.clusters:
            for instance in cluster.instances:
                port = _free_port()
                self.port_map[instance.instance_id] = port
                # demo_config 已用默认端口构造 URL；需要按实际端口重建配置
        # 用实际端口重新生成配置
        self.config, self.health_settings = demo_config(self.port_map)
        for cluster in self.config.clusters:
            for instance in cluster.instances:
                port = self.port_map[instance.instance_id]
                state = SimState(instance.instance_id, cluster.name)
                self.handles[instance.instance_id] = InstanceHandle(
                    instance_id=instance.instance_id,
                    cluster=cluster.name,
                    port=port,
                    base_url=instance.base_url,
                    state=state,
                )

    def start_all(self) -> None:
        for handle in self.handles.values():
            handle.start()
        # 等待所有端口真正可连
        deadline = time.time() + 10
        for handle in self.handles.values():
            while time.time() < deadline and not _ping(handle.base_url):
                time.sleep(0.02)

    def stop_all(self) -> None:
        for handle in self.handles.values():
            handle.stop()

    def get(self, instance_id: str) -> InstanceHandle:
        return self.handles[instance_id]

    def set_healthy(self, instance_id: str, healthy: bool) -> None:
        url = self.handles[instance_id].base_url + "/_sim/set_healthy"
        httpx.post(url, json={"healthy": healthy}, timeout=2.0)

    def set_behaviour(
        self, instance_id: str, mode: str, seconds: float = 0.0
    ) -> None:
        url = self.handles[instance_id].base_url + "/_sim/set_behaviour"
        httpx.post(
            url, json={"mode": mode, "seconds": seconds}, timeout=2.0
        )

    def state(self, instance_id: str) -> dict:
        url = self.handles[instance_id].base_url + "/_sim/state"
        return httpx.get(url, timeout=2.0).json()


def _ping(base_url: str) -> bool:
    try:
        response = httpx.get(base_url + "/healthz", timeout=0.5)
        return response.status_code == 200
    except httpx.HTTPError:
        return False


def main() -> None:
    cluster = SimCluster()
    cluster.start_all()
    print("simulated upstreams:")
    for handle in cluster.handles.values():
        print(f"  {handle.instance_id:14s} {handle.base_url}")

    from gateway.app import create_app

    app = create_app(cluster.config, cluster.health_settings)
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=8080, log_level="info")
    )
    print("gateway listening on http://127.0.0.1:8080")
    try:
        server.run()
    finally:
        cluster.stop_all()


if __name__ == "__main__":
    main()
