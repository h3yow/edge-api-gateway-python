"""FastAPI 统一入口。

同步的网关核心在 Starlette 的 worker 线程池中执行
（``run_in_threadpool``），因此并发 HTTP 请求会落到不同线程，
真实地检验锁与状态机的并发正确性。

端点：

* ``/{path}``（全方法）：统一代理入口；
* ``GET /_gw/health``：实例健康视图与摘除/恢复事件；
* ``GET /_gw/callers/{caller_id}``：调用方限流/配额视图；
* ``POST /_gw/explain``：对给定特征打印规则判定表，不转发。

响应头携带 ``X-Gateway-Rule`` 与 ``X-Gateway-Instance``，
便于在本地直接核对“命中规则 + 最终选择的实例”。
"""

from __future__ import annotations

import contextlib
import threading

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from starlette.concurrency import run_in_threadpool

from gateway.config import HealthSettings, demo_config
from gateway.errors import GatewayError
from gateway.models import GatewayConfig, RequestFeature
from gateway.proxy import Gateway

_ALL_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]

#: 逐跳/框架头不向上游透传，也不回带
_HOP_BY_HOP = {"host", "content-length", "connection", "keep-alive",
               "transfer-encoding", "te", "upgrade"}


def create_app(
    config: GatewayConfig | None = None,
    health_settings: HealthSettings | None = None,
    *,
    enable_probe_loop: bool = True,
    probe_interval: float = 0.2,
) -> FastAPI:
    if config is None:
        config, health_settings = demo_config()
    health_settings = health_settings or HealthSettings()

    gateway = Gateway(config, health_settings)
    stop_event = threading.Event()

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        thread: threading.Thread | None = None
        if enable_probe_loop:
            def _probe_loop() -> None:
                while not stop_event.wait(probe_interval):
                    try:
                        gateway.probe_all()
                    except Exception:  # 探测循环绝不能因异常退出
                        pass

            thread = threading.Thread(
                target=_probe_loop, name="gateway-probe", daemon=True
            )
            thread.start()
        try:
            yield
        finally:
            stop_event.set()
            if thread is not None:
                thread.join(timeout=2)
            gateway.close()

    app = FastAPI(title="edge-api-gateway", lifespan=lifespan)
    app.state.gateway = gateway

    def _feature_from_request(request: Request, body: bytes) -> RequestFeature:
        headers = {
            k: v
            for k, v in request.headers.items()
            if k.lower() not in _HOP_BY_HOP
        }
        return RequestFeature(
            method=request.method,
            path=request.url.path,
            headers=headers,
            body=body,
        )

    # ---- 管理端点（必须先于 catch-all 注册）----
    @app.get("/_gw/health")
    def gw_health() -> dict:
        return gateway.health_snapshot()

    @app.get("/_gw/callers/{caller_id}")
    def gw_caller(caller_id: str) -> JSONResponse:
        from gateway.errors import caller_unidentified

        if caller_id not in config.caller_policies:
            err = caller_unidentified()
            return JSONResponse(err.to_payload(), status_code=err.http_status)
        return JSONResponse(gateway.caller_snapshot(caller_id))

    @app.post("/_gw/explain")
    async def gw_explain(request: Request) -> JSONResponse:
        payload = await request.json()
        feature = RequestFeature(
            method=payload.get("method", "GET"),
            path=payload.get("path", "/"),
            headers={str(k): str(v) for k, v in payload.get("headers", {}).items()},
        )
        return JSONResponse({"rules": gateway.explain_rules(feature)})

    # ---- 统一代理入口 ----
    @app.api_route(
        "/{full_path:path}",
        methods=_ALL_METHODS,
        include_in_schema=False,
    )
    async def proxy(full_path: str, request: Request) -> Response:
        body = await request.body()
        feature = _feature_from_request(request, body)
        try:
            result = await run_in_threadpool(gateway.handle, feature)
        except GatewayError as exc:
            return JSONResponse(
                exc.to_payload(),
                status_code=exc.http_status,
            )
        headers: dict[str, str] = {}
        if result.headers.get("content-type"):
            headers["content-type"] = result.headers["content-type"]
        headers["X-Gateway-Rule"] = result.rule_id
        headers["X-Gateway-Instance"] = result.instance_id
        return Response(
            content=result.body,
            status_code=result.status,
            headers=headers,
        )

    return app


app = create_app()
