"""FastAPI application exposing the unified gateway entry point.

Every request, regardless of path, enters one handler (both ``/`` and
``/gateway/...`` map to it), is normalized into a :class:`RequestContext`
and decided by the same pipeline, so identical requests are always decided
identically.

Operational endpoints (not forwarded):
* ``GET  /healthz``              - liveness of the gateway process
* ``GET  /__routes``            - configured rules and cluster health
* ``GET  /__explain?...``        - routing explanation without dispatching
* ``POST /__admin/faults``       - inject/clear simulated upstream faults
* ``GET  /__admin/state``        - quota + health snapshot
* ``POST /__admin/reset``        - reset quotas, health, counters and faults
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from edge_gateway.config import default_clusters, default_rules
from edge_gateway.errors import GatewayError
from edge_gateway.gateway import Gateway
from edge_gateway.health import HealthMonitor
from edge_gateway.models import RequestContext
from edge_gateway.quota import QuotaManager
from edge_gateway.router import Router, RuleRegistry
from edge_gateway.upstream import UpstreamRegistry, build_default_registry

ADMIN_PREFIXES = ("/__",)


def build_default_gateway() -> Gateway:
    clusters = default_clusters()
    registry = build_default_registry(clusters)
    instances = [instance_id for ids in clusters.values() for instance_id in ids]
    rules = default_rules()
    return Gateway(
        router=Router(RuleRegistry(rules)),
        quotas=QuotaManager(),
        health=HealthMonitor(instances, registry),
        registry=registry,
        clusters=clusters,
    )


async def _normalize(request: Request, path: str) -> RequestContext:
    raw = await request.body()
    headers = {k.lower(): v for k, v in request.headers.items()}
    query = {k: v for k, v in request.query_params.items()}
    caller = headers.get("x-caller-id", "anonymous")
    return RequestContext(
        method=request.method.upper(),
        path="/" + path.lstrip("/"),
        headers=headers,
        query=query,
        caller=caller,
        body=raw,
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


def create_app(gateway: Gateway | None = None) -> FastAPI:
    gateway = gateway or build_default_gateway()
    app = FastAPI(title="edge-api-gateway", version="0.1.0")
    app.state.gateway = gateway

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"ok": True}

    @app.get("/__routes")
    async def list_routes() -> dict:
        rules = [
            {
                "id": r.id,
                "priority": r.priority,
                "cluster": r.cluster,
                "path_prefix": r.path_prefix,
                "methods": sorted(r.methods) if r.methods else None,
                "header_equals": dict(r.header_equals),
                "query_equals": dict(r.query_equals),
                "callers": sorted(r.callers) if r.callers else None,
                "quota_limit": r.quota_limit,
                "quota_window_seconds": r.quota_window_seconds,
            }
            for r in gateway._router._registry.rules  # noqa: SLF001
        ]
        return {"rules": rules, "clusters": gateway._clusters}  # noqa: SLF001

    @app.get("/__explain")
    async def explain(request: Request) -> JSONResponse:
        path = request.query_params.get("path", "/")
        ctx = RequestContext(
            method=request.query_params.get("method", "GET").upper(),
            path=path if path.startswith("/") else "/" + path,
            headers={
                k.lower(): v
                for k, v in request.headers.items()
                if k.lower().startswith("x-")
            },
            query={
                k: v for k, v in request.query_params.items() if k not in ("path", "method")
            },
            caller=request.headers.get("x-caller-id", "anonymous"),
        )
        return JSONResponse(gateway.explain(ctx))

    @app.get("/__admin/state")
    async def admin_state() -> dict:
        return {
            "health": gateway._health.snapshot(),  # noqa: SLF001
        }

    @app.post("/__admin/faults")
    async def admin_faults(request: Request) -> JSONResponse:
        payload = await request.json()
        instance_id = payload.get("instance_id")
        fault = payload.get("fault")
        try:
            gateway._registry.set_fault(instance_id, fault)  # noqa: SLF001
        except (KeyError, ValueError) as exc:
            return JSONResponse(status_code=400, content={"ok": False, "error": str(exc)})
        return {"ok": True, "instance_id": instance_id, "fault": fault}

    @app.post("/__admin/reset")
    async def admin_reset() -> dict:
        gateway._quotas.reset()  # noqa: SLF001
        gateway._health.reset()  # noqa: SLF001
        gateway._registry.reset_all()  # noqa: SLF001
        return {"ok": True}

    async def gateway_entry(request: Request, path: str = "") -> JSONResponse:
        ctx = await _normalize(request, path)
        try:
            result = gateway.handle(ctx)
        except GatewayError as exc:
            return _error_response(exc)
        return JSONResponse(
            status_code=result.status_code,
            content={
                "ok": True,
                "rule_id": result.rule_id,
                "cluster": result.cluster,
                "instance_id": result.instance_id,
                "upstream_status": result.upstream_status,
                "data": result.upstream_body,
                "quota": result.quota,
                "attempts": [
                    {
                        "instance_id": a.instance_id,
                        "outcome": a.outcome,
                        "detail": a.detail,
                        "retried": a.retried,
                    }
                    for a in result.attempts
                ],
                "trace": result.trace,
            },
        )

    # Two entry points, identical behavior: proves any entry decides alike.
    app.api_route("/gateway/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])(
        gateway_entry
    )
    app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])(
        gateway_entry
    )
    return app


app = create_app()
