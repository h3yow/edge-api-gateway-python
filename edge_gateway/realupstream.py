"""Real network upstreams: independently started local HTTP processes.

Unlike the in-process simulated registry (``upstream.py``), upstreams here
are separate OS processes reached over real TCP sockets. Stopping a process
yields a genuine connection refusal, a slowed process yields a genuine socket
wait, and every transport failure therefore carries real network semantics:

* process stopped / restarting -> connection refused ->
  :class:`~edge_gateway.upstream.UpstreamUnreachable` (handler never ran,
  failover is safe for every method);
* process too slow to answer -> socket timeout ->
  :class:`~edge_gateway.upstream.UpstreamTimeout` (execution unconfirmed,
  non-idempotent requests are never retried);
* process answers 5xx -> the raw status and body are returned to the gateway,
  which treats it as "executed once".

Each process is an echo server: it answers *every* method/path and reports
the exact method, path, query, common headers and body it received, while a
small local admin API (`/__admin/*`, `/__health`) allows the drill tooling to
slow it down, make it error, or observe its user-request execution count.
"""

from __future__ import annotations

import argparse
import base64
import http.client
import json
import socket
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Mapping
from urllib.parse import urlsplit, urlencode

from edge_gateway.models import RequestContext
from edge_gateway.upstream import UpstreamError, UpstreamTimeout, UpstreamUnreachable

# Hop-by-hop / transport headers that must not be re-issued verbatim.
_HOP_BY_HOP = frozenset(
    {
        "host",
        "connection",
        "keep-alive",
        "proxy-connection",
        "proxy-authenticate",
        "proxy-authorization",
        "transfer-encoding",
        "te",
        "trailer",
        "upgrade",
        "content-length",
    }
)


@dataclass
class UpstreamResponse:
    """Raw upstream answer: status code, body bytes and response headers."""

    status: int
    body: bytes = b""
    headers: Mapping[str, str] = field(default_factory=dict)


class HttpUpstreamRegistry:
    """Network-backed registry mapping instance ids to real process URLs."""

    def __init__(self, request_timeout: float = 5.0, probe_timeout: float = 1.0) -> None:
        self._bases: dict[str, str] = {}
        self._clusters: dict[str, str] = {}
        self._request_timeout = request_timeout
        self._probe_timeout = probe_timeout

    def register(self, instance_id: str, base_url: str, cluster: str) -> None:
        self._bases[instance_id] = base_url.rstrip("/")
        self._clusters[instance_id] = cluster

    def base_url_of(self, instance_id: str) -> str:
        if instance_id not in self._bases:
            raise UpstreamUnreachable(f"unknown instance {instance_id!r}")
        return self._bases[instance_id]

    def cluster_of(self, instance_id: str) -> str | None:
        return self._clusters.get(instance_id)

    def instance_ids(self) -> list[str]:
        return list(self._bases)

    # -- HTTP transport ----------------------------------------------------

    def _connect(self, instance_id: str, timeout: float) -> tuple[http.client.HTTPConnection, str]:
        parts = urlsplit(self.base_url_of(instance_id))
        host = parts.hostname or "127.0.0.1"
        port = parts.port or (443 if parts.scheme == "https" else 80)
        return http.client.HTTPConnection(host, port, timeout=timeout), parts.scheme

    @staticmethod
    def _forward_headers(headers: Mapping[str, str]) -> dict[str, str]:
        return {k: v for k, v in headers.items() if k.lower() not in _HOP_BY_HOP}

    @staticmethod
    def _target_path(request: RequestContext) -> str:
        if request.query:
        # Re-encode the normalized query mapping for the network hop.
            return request.path + "?" + urlencode(list(request.query.items()))
        return request.path

    def call(self, instance_id: str, request: RequestContext) -> UpstreamResponse:
        """Forward one request over the network and return the raw answer."""

        conn, _scheme = self._connect(instance_id, self._request_timeout)
        try:
            conn.request(
                request.method,
                self._target_path(request),
                body=request.body,
                headers=self._forward_headers(request.headers),
            )
            resp = conn.getresponse()
            body = resp.read()
            headers = {k.lower(): v for k, v in resp.getheaders()}
            return UpstreamResponse(resp.status, body, headers)
        except UpstreamError:
            raise
        except socket.timeout as exc:
            raise UpstreamTimeout(f"{instance_id} did not answer in time") from exc
        except (ConnectionError, OSError, http.client.HTTPException) as exc:
            # Stopped process, restart window (connection refused/reset),
            # broken pipe: the handler definitely never produced an answer.
            raise UpstreamUnreachable(f"{instance_id} unreachable: {exc}") from exc
        finally:
            conn.close()

    def probe(self, instance_id: str) -> None:
        """GET /__health over the network; raises on anything but HTTP 200."""

        conn, _scheme = self._connect(instance_id, self._probe_timeout)
        try:
            conn.request("GET", "/__health")
            resp = conn.getresponse()
            resp.read()
            status = resp.status
        except UpstreamError:
            raise
        except socket.timeout as exc:
            raise UpstreamTimeout(f"{instance_id} health probe timed out") from exc
        except (ConnectionError, OSError, http.client.HTTPException) as exc:
            raise UpstreamUnreachable(f"{instance_id} probe unreachable: {exc}") from exc
        finally:
            conn.close()
        if status != 200:
            raise UpstreamError(f"{instance_id} health probe got HTTP {status}")

    # -- remote admin (used by drill tooling) ------------------------------

    def _admin_get(self, instance_id: str, path: str, timeout: float) -> dict:
        conn, _ = self._connect(instance_id, timeout)
        try:
            conn.request("GET", path)
            resp = conn.getresponse()
            raw = resp.read()
            if resp.status != 200:
                raise UpstreamError(f"{instance_id} admin GET {path} -> HTTP {resp.status}")
            return json.loads(raw.decode("utf-8"))
        finally:
            conn.close()

    def _admin_post(self, instance_id: str, path: str, payload: dict, timeout: float) -> dict:
        data = json.dumps(payload).encode("utf-8")
        conn, _ = self._connect(instance_id, timeout)
        try:
            conn.request(
                "POST",
                path,
                body=data,
                headers={"content-type": "application/json", "content-length": str(len(data))},
            )
            resp = conn.getresponse()
            raw = resp.read()
            if resp.status // 100 != 2:
                raise UpstreamError(f"{instance_id} admin POST {path} -> HTTP {resp.status}")
            return json.loads(raw.decode("utf-8")) if raw else {}
        finally:
            conn.close()

    def set_remote_mode(
        self, instance_id: str, mode: str, delay: float = 1.0, timeout: float = 2.0
    ) -> dict:
        return self._admin_post(
            instance_id, "/__admin/mode", {"mode": mode, "delay": delay}, timeout
        )

    def remote_state(self, instance_id: str, timeout: float = 2.0) -> dict:
        return self._admin_get(instance_id, "/__admin/state", timeout)


# --------------------------------------------------------------------------
# The upstream process itself
# --------------------------------------------------------------------------


class _ModeState:
    """Process-wide fault mode, shared by the threaded HTTP server."""

    def __init__(self, instance_id: str) -> None:
        self.instance_id = instance_id
        self._lock = threading.Lock()
        self.mode = "normal"  # normal | slow | error
        self.delay = 1.0
        self.calls = 0

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "instance_id": self.instance_id,
                "mode": self.mode,
                "delay": self.delay,
                "calls": self.calls,
            }

    def set_mode(self, mode: str, delay: float) -> None:
        with self._lock:
            self.mode = mode
            self.delay = delay

    def current(self) -> tuple[str, float]:
        with self._lock:
            return self.mode, self.delay

    def count_call(self) -> int:
        with self._lock:
            self.calls += 1
            return self.calls


def _make_handler(state: _ModeState) -> type[BaseHTTPRequestHandler]:
    class EchoHandler(BaseHTTPRequestHandler):
        server_version = "edge-upstream/1.0"

        # Keep drill output clean; the parent logs every request to stderr.
        def log_message(self, fmt: str, *args) -> None:  # noqa: A003
            return

        # -- helpers -------------------------------------------------------

        def _send_json(self, status: int, payload: dict) -> None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("content-type", "application/json; charset=utf-8")
            self.send_header("content-length", str(len(data)))
            self.send_header("x-served-by", state.instance_id)
            self.end_headers()
            self.wfile.write(data)

        def _read_body(self) -> bytes:
            length = int(self.headers.get("content-length") or 0)
            return self.rfile.read(length) if length else b""

        # -- routes --------------------------------------------------------

        def _handle_admin(self, method: str) -> None:
            if self.path == "/__health":
                mode, _delay = state.current()
                if mode == "error":
                    self._send_json(503, {"ok": False, "instance_id": state.instance_id})
                else:
                    # 'slow' mode slows user traffic only; a live slow
                    # process is still a live process for health probing.
                    self._send_json(200, {"ok": True, "instance_id": state.instance_id})
                return
            if self.path == "/__admin/state" and method == "GET":
                self._send_json(200, state.snapshot())
                return
            if self.path == "/__admin/mode" and method == "POST":
                try:
                    payload = json.loads(self._read_body().decode("utf-8"))
                    mode = str(payload.get("mode", "normal"))
                    if mode not in ("normal", "slow", "error"):
                        self._send_json(400, {"ok": False, "error": f"bad mode {mode!r}"})
                        return
                    delay = float(payload.get("delay", 1.0))
                except (ValueError, UnicodeDecodeError):
                    self._send_json(400, {"ok": False, "error": "invalid json payload"})
                    return
                state.set_mode(mode, delay)
                self._send_json(200, {"ok": True, **state.snapshot()})
                return
            self._send_json(404, {"ok": False, "error": "no such admin route"})

        def _handle_user(self, method: str) -> None:
            mode, delay = state.current()
            if mode == "slow":
                # Real, network-visible slowness: the socket simply waits.
                state.count_call()
                threading.Event().wait(delay)
                self._send_json(
                    200,
                    {"instance_id": state.instance_id, "note": "slow response completed"},
                )
                return

            body = self._read_body()
            n = state.count_call()
            if mode == "error":
                self._send_json(
                    500,
                    {"ok": False, "error": "simulated upstream error",
                     "instance_id": state.instance_id, "calls": n},
                )
                return

            # Echo exactly what was received.
            try:
                body_text = body.decode("utf-8")
            except UnicodeDecodeError:
                body_text = None
            self._send_json(
                200,
                {
                    "instance_id": state.instance_id,
                    "method": method,
                    "path": self.path.split("?", 1)[0],
                    "query": self.path.split("?", 1)[1] if "?" in self.path else "",
                    "headers": {k.lower(): v for k, v in self.headers.items()},
                    "body_text": body_text,
                    "body_base64": base64.b64encode(body).decode("ascii"),
                    "body_bytes": len(body),
                    "calls": n,
                },
            )

        def _dispatch(self, method: str) -> None:
            if self.path == "/__health" or self.path.startswith("/__admin/"):
                self._handle_admin(method)
            else:
                self._handle_user(method)

        def do_GET(self) -> None:  # noqa: N802
            self._dispatch("GET")

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch("POST")

        def do_PUT(self) -> None:  # noqa: N802
            self._dispatch("PUT")

        def do_PATCH(self) -> None:  # noqa: N802
            self._dispatch("PATCH")

        def do_DELETE(self) -> None:  # noqa: N802
            self._dispatch("DELETE")

        def do_HEAD(self) -> None:  # noqa: N802
            self._dispatch("HEAD")

        def do_OPTIONS(self) -> None:  # noqa: N802
            self._dispatch("OPTIONS")

    return EchoHandler


def run_upstream_server(instance_id: str, host: str, port: int) -> None:
    """Run one echo upstream HTTP server in this process until interrupted."""

    state = _ModeState(instance_id)
    server = ThreadingHTTPServer((host, port), _make_handler(state))
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server.server_close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run one edge-gateway upstream process.")
    parser.add_argument("--id", dest="instance_id", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args(argv)
    run_upstream_server(args.instance_id, args.host, args.port)


if __name__ == "__main__":
    main()
