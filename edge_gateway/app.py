"""FastAPI application exposing the unified gateway entry point.

Every request, regardless of path, enters one handler (both ``/`` and
``/gateway/...`` map to it), is normalized into a :class:`RequestContext`
and decided by the same pipeline, so identical requests are always decided
identically.

Two upstream transports are supported:

* ``create_app()`` / ``app`` - in-process simulated upstreams (no external
  process needed); successful answers are wrapped in an explainable JSON
  envelope.
* ``network_app`` - upstreams are independent local HTTP processes (see
  ``edge_gateway.realupstream`` / ``python -m edge_gateway.drill up``). The
  upstream's status code, body and response headers are returned to the
  caller verbatim; a background thread restores removed instances even with
  no traffic.

Concurrency model
-----------------
The gateway pipeline (``Gateway.handle``) is fully synchronous and, in
network mode, performs blocking socket I/O via ``http.client``. It is
therefore **never run on the asyncio event loop**: every request is handed
to a dedicated bounded thread pool (``EG_DISPATCH_WORKERS``, default 32).
Consequences:

* multiple requests arriving at the same instant - including several stuck
  behind one slow upstream - are genuinely processed in parallel;
* one frozen upstream occupies only a worker thread (bounded by the request
  timeout), never the event loop, so ``/healthz`` and the other operational
  endpoints, as well as requests that never touch an upstream, stay
  responsive while a slow request is in flight;
* admission (rate/inflight/quota) is decided before the upstream call, so
  concurrent requests against slow or restarting upstreams can never
  over-admit any category.

Operational endpoints (not forwarded):
* ``GET  /healthz``              - liveness of the gateway process
* ``GET  /__routes``            - configured rules and cluster health
* ``GET  /__explain?...``        - routing explanation without dispatching
* ``POST /__admin/faults``       - inject/clear simulated or network faults
* ``GET  /__admin/state``        - admission + health snapshot
* ``POST /__admin/reset``        - reset admission, health and faults
"""

from __future__ import annotations

import asyncio
import json
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from edge_gateway.admission import AdmissionManager
from edge_gateway.config import (
    default_clusters,
    default_network_base_urls,
    default_rules,
)
from edge_gateway.errors import GatewayError
from edge_gateway.gateway import Gateway
from edge_gateway.health import HealthMonitor
from edge_gateway.models import RequestContext
from edge_gateway.realupstream import HttpUpstreamRegistry
from edge_gateway.recovery import RecoveryLoop
from edge_gateway.router import Router, RuleRegistry
from edge_gateway.upstream import build_default_registry

ADMIN_PREFIXES = ("/__",)

# Network mode tunes itself for real processes: a trip needs more than one
# blip, recovery needs a confirming streak, and the background loop probes
# often enough to demonstrate self-healing within seconds.
NETWORK_FAILURE_THRESHOLD = 2
NETWORK_RECOVERY_THRESHOLD = 2
NETWORK_PROBE_INTERVAL = 0.5
NETWORK_RECOVERY_INTERVAL = 0.25
NETWORK_REQUEST_TIMEOUT = float(os.environ.get("EG_REQUEST_TIMEOUT", "2.0"))

# The gateway pipeline is fully synchronous and performs blocking socket I/O
# in network mode (http.client). It must therefore never run on the asyncio
# event loop: every inbound request is handed to this dedicated bounded pool
# so that
#   * multiple requests - including multiple requests stuck behind one slow
#     upstream - are genuinely processed in parallel;
#   * a frozen upstream occupies only a worker thread, never the event loop,
#     so /healthz and the operational endpoints stay responsive;
#   * the pool bound caps how many blocked upstreams can accumulate.
DISPATCH_WORKERS = int(os.environ.get("EG_DISPATCH_WORKERS", "32"))


def _new_gateway_components(registry, clusters, health_kwargs, instances=None):
    rules = default_rules()
    if instances is None:
        instances = [
            instance_id for ids in clusters.values() for instance_id in ids
        ]
    health = HealthMonitor(instances, registry, **health_kwargs)
    return rules, health, AdmissionManager()


def build_default_gateway() -> Gateway:
    clusters = default_clusters()
    registry = build_default_registry(clusters)
    rules, health, admission = _new_gateway_components(
        registry, clusters, dict(failure_threshold=3, recovery_threshold=2,
                                 probe_interval=0.05)
    )
    return Gateway(
        router=Router(RuleRegistry(rules)),
        quotas=admission,
        health=health,
        registry=registry,
        clusters=clusters,
    )


def build_network_gateway(
    base_urls: dict[str, str] | None = None,
    start_recovery: bool = True,
    clusters: dict[str, list[str]] | None = None,
) -> tuple[Gateway, RecoveryLoop | None]:
    """Gateway talking to real upstream processes over TCP.

    ``clusters`` optionally restricts the topology to a subset (instances
    absent from it are simply not registered).
    """

    clusters = clusters or default_clusters()
    base_urls = base_urls or default_network_base_urls()
    registry = HttpUpstreamRegistry(request_timeout=NETWORK_REQUEST_TIMEOUT)
    instances: list[str] = []
    for cluster, cluster_instances in clusters.items():
        for instance_id in cluster_instances:
            registry.register(instance_id, base_urls[instance_id], cluster)
            instances.append(instance_id)
    rules, health, admission = _new_gateway_components(
        registry,
        clusters,
        dict(
            failure_threshold=NETWORK_FAILURE_THRESHOLD,
            recovery_threshold=NETWORK_RECOVERY_THRESHOLD,
            probe_interval=NETWORK_PROBE_INTERVAL,
        ),
        instances=instances,
    )
    recovery = RecoveryLoop(health, interval=NETWORK_RECOVERY_INTERVAL)
    gateway = Gateway(
        router=Router(RuleRegistry(rules)),
        quotas=admission,
        health=health,
        registry=registry,
        clusters=clusters,
        transparent=True,
        recovery=recovery,
    )
    if start_recovery:
        recovery.start()
    return gateway, recovery


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
    return _build_app(gateway, network_mode=False, recovery=None)


def _build_app(
    gateway: Gateway, network_mode: bool, recovery: RecoveryLoop | None
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            yield
        finally:
            # Drop queued work; in-flight upstream calls are bounded by the
            # request timeout, so this wait is itself bounded.
            dispatcher.shutdown(wait=True, cancel_futures=True)
            if recovery is not None:
                recovery.stop()

    # Dedicated pool: blocking gateway pipeline (and network admin calls)
    # never touch the event loop thread.
    dispatcher = ThreadPoolExecutor(
        max_workers=DISPATCH_WORKERS, thread_name_prefix="eg-dispatch"
    )

    async def run_offloaded(func, *args):
        # Grab the running loop per call; _build_app itself runs outside any
        # event loop, so the loop cannot be captured here.
        return await asyncio.get_running_loop().run_in_executor(
            dispatcher, func, *args
        )

    app = FastAPI(
        title="edge-api-gateway", version="0.2.0", lifespan=lifespan
    )
    app.state.gateway = gateway
    app.state.recovery = recovery
    app.state.dispatcher = dispatcher

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"ok": True, "mode": "network" if network_mode else "simulated"}

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
                "rate_limit": r.rate_limit,
                "rate_window_seconds": r.rate_window_seconds,
                "inflight_limit": r.inflight_limit,
            }
            for r in gateway._router._registry.rules  # noqa: SLF001
        ]
        return {
            "mode": "network" if network_mode else "simulated",
            "rules": rules,
            "clusters": gateway._clusters,  # noqa: SLF001
        }

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
            "mode": "network" if network_mode else "simulated",
            "health": gateway._health.snapshot(),  # noqa: SLF001
        }

    @app.post("/__admin/faults")
    async def admin_faults(request: Request) -> JSONResponse:
        payload = await request.json()
        instance_id = payload.get("instance_id")
        fault = payload.get("fault")

        def apply_fault():
            if network_mode:
                # Map the legacy fault names onto the real-process modes.
                if fault in ("fail", "error"):
                    gateway._registry.set_remote_mode(instance_id, "error")  # noqa: SLF001
                elif fault == "slow":
                    delay = float(payload.get("delay", 1.0))
                    gateway._registry.set_remote_mode(  # noqa: SLF001
                        instance_id, "slow", delay=delay
                    )
                else:
                    # 'down' cannot be emulated by a live process; only the
                    # drill CLI can truly stop it. 'timeout'/None -> normal.
                    gateway._registry.set_remote_mode(instance_id, "normal")  # noqa: SLF001
            else:
                gateway._registry.set_fault(instance_id, fault)  # noqa: SLF001

        try:
            # set_remote_mode is itself a blocking network call in network
            # mode; keep it off the event loop too.
            await run_offloaded(apply_fault)
        except (KeyError, ValueError) as exc:
            return JSONResponse(
                status_code=400, content={"ok": False, "error": str(exc)}
            )
        return {"ok": True, "instance_id": instance_id, "fault": fault}

    @app.post("/__admin/reset")
    async def admin_reset() -> dict:
        gateway._quotas.reset()  # noqa: SLF001
        gateway._health.reset()  # noqa: SLF001
        if not network_mode:
            gateway._registry.reset_all()  # noqa: SLF001
        return {"ok": True}

    async def gateway_entry(request: Request, path: str = "") -> Response:
        ctx = await _normalize(request, path)
        try:
            # The whole synchronous pipeline (including blocking upstream
            # socket I/O) runs on a worker thread, so concurrent requests are
            # dispatched in parallel and a frozen upstream cannot block the
            # event loop nor the operational endpoints.
            result = await run_offloaded(gateway.handle, ctx)
        except GatewayError as exc:
            return _error_response(exc)

        if result.transparent:
            # Real network mode: upstream status + body + headers verbatim.
            headers = {
                k: v
                for k, v in result.raw_headers.items()
                if k.lower()
                not in (
                    "content-length",
                    "transfer-encoding",
                    "connection",
                    "content-encoding",
                )
            }
            headers["x-gateway-rule"] = result.rule_id
            headers["x-gateway-instance"] = result.instance_id
            return Response(
                content=result.raw_body,
                status_code=result.status_code,
                headers=headers,
                media_type=headers.get("content-type"),
            )

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
                "admission": result.admission,
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
    app.api_route("/gateway/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"])(
        gateway_entry
    )
    app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"])(
        gateway_entry
    )
    return app


def create_network_app(
    base_urls: dict[str, str] | None = None,
    start_recovery: bool = True,
    clusters: dict[str, list[str]] | None = None,
) -> FastAPI:
    gateway, recovery = build_network_gateway(base_urls, start_recovery, clusters)
    return _build_app(gateway, network_mode=True, recovery=recovery)


# Lazy module-level network app for `uvicorn edge_gateway.app:network_app`.
# Upstream processes are started separately by the drill tooling.
_network_app: FastAPI | None = None


def __getattr__(name: str):  # PEP 562 lazy attribute
    if name == "network_app":
        global _network_app
        if _network_app is None:
            _network_app = create_network_app()
        return _network_app
    raise AttributeError(name)


app = create_app()
