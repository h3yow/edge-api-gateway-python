"""FastAPI gateway application that proxies to real network upstreams.

Unlike the in-process demo app, this one:

* forwards the raw method/path/query/body/headers and returns the upstream
  status code, body and headers transparently;
* starts a :class:`~edge_gateway.prober.BackgroundProber` so removed
  instances heal on their own even with no traffic;
* enforces all three admission categories through
  :class:`~edge_gateway.admission.AdmissionManager`.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from edge_gateway.admission import AdmissionManager
from edge_gateway.errors import GatewayError
from edge_gateway.gateway import Gateway
from edge_gateway.health import HealthMonitor
from edge_gateway.http_upstream import HOP_BY_HOP, HttpUpstreamRegistry
from edge_gateway.models import RequestContext
from edge_gateway.prober import BackgroundProber
from edge_gateway.router import Router, RuleRegistry
from edge_gateway.lab import lab_config


def build_network_gateway(
    ports: dict[str, int] | None = None,
    failure_threshold: int = 3,
    recovery_threshold: int = 2,
    probe_interval: float = 0.1,
    timeout: float = 2.0,
) -> Gateway:
    ports = ports or lab_config.default_ports()
    clusters = lab_config.default_clusters()
    registry = HttpUpstreamRegistry(lab_config.base_urls(ports), timeout=timeout)
    instances = [i for ids in clusters.values() for i in ids]
    rules = lab_config.default_rules()
    return Gateway(
        router=Router(RuleRegistry(rules)),
        quotas=AdmissionManager(),
        health=HealthMonitor(
            instances,
            registry,
            failure_threshold=failure_threshold,
            recovery_threshold=recovery_threshold,
            probe_interval=probe_interval,
        ),
        registry=registry,
        clusters=clusters,
    )


def _error_response(exc: GatewayError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "ok": False,
            "error": {
                "code": exc.code,
                "message": exc.message,
                "details": exc.details,
            },
        },
    )


async def _normalize(request: Request) -> RequestContext:
    raw = await request.body()
    headers = {k.lower(): v for k, v in request.headers.items()}
    # Preserve the raw query string (repeating keys etc.).
    query_items = {k: v for k, v in request.query_params.multi_items()}
    caller = headers.get("x-caller-id", "anonymous")
    return RequestContext(
        method=request.method.upper(),
        path=request.url.path,
        headers=headers,
        query=query_items,
        caller=caller,
        body=raw,
    )


def create_network_app(
    gateway: Gateway | None = None,
    prober_interval: float = 0.05,
    start_prober: bool = True,
) -> FastAPI:
    gateway = gateway or build_network_gateway()

    prober = BackgroundProber(gateway._health, interval=prober_interval)  # noqa: SLF001

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if start_prober:
            prober.start()
        try:
            yield
        finally:
            prober.stop()
            registry = getattr(gateway._registry, "close", None)  # noqa: SLF001
            if callable(registry):
                registry()

    app = FastAPI(title="edge-api-gateway-network-lab", version="0.2.0", lifespan=lifespan)
    app.state.gateway = gateway
    app.state.prober = prober

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"ok": True, "prober_running": prober.running}

    @app.get("/__routes")
    async def list_routes() -> dict:
        return {"clusters": gateway._clusters}  # noqa: SLF001

    @app.get("/__admin/state")
    async def admin_state() -> dict:
        return {
            "health": gateway._health.snapshot(),  # noqa: SLF001
            "events": gateway._health.events(),  # noqa: SLF001
            "prober_running": prober.running,
        }

    @app.post("/__admin/reset")
    async def admin_reset() -> dict:
        gateway._quotas.reset()  # noqa: SLF001
        gateway._health.reset()  # noqa: SLF001
        return {"ok": True}

    async def proxy_entry(request: Request) -> Response:
        ctx = await _normalize(request)
        # Dispatch is synchronous and may block on slow upstreams; run it in
        # a worker thread so the event loop (and thus concurrent requests)
        # is never serialized behind one upstream call.
        try:
            result = await asyncio.to_thread(gateway.handle, ctx)
        except GatewayError as exc:
            return _error_response(exc)

        # Transparent pass-back: upstream status, body and content headers.
        headers = {
            k: v
            for k, v in result.upstream_headers.items()
            if k.lower() not in HOP_BY_HOP
        }
        headers["x-gateway-rule"] = result.rule_id
        headers["x-gateway-instance"] = result.instance_id
        return Response(
            status_code=result.upstream_status,
            content=result.upstream_body_raw,
            headers=headers,
        )

    app.api_route(
        "/gateway/{path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
    )(proxy_entry)
    app.api_route(
        "/{path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
    )(proxy_entry)
    return app
