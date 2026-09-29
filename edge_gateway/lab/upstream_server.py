"""A standalone upstream HTTP server, runnable as its own process.

Each instance is a plain ``http.server`` based service on its own port:

* Business requests (any method/path outside ``/__``) echo exactly what the
  gateway forwarded - method, path, raw query, common headers and body - so
  passthrough is directly assertable. A served business request increments a
  counter (``GET /__meta``) so at-most-once execution can be checked.
* Local control endpoints change behavior at runtime, no restart needed:

    POST /__control/mode  {"mode": "normal"|"slow"|"error"|"status",
                           "delay": 1.5, "status": 418}
    GET  /__control/state
    GET  /__meta                          # served business request count
    GET  /__health                        # probe target

Fault semantics:

* **normal** - business requests answer 200, health probe answers 200.
* **slow**   - business requests sleep ``delay`` seconds before answering;
               the health probe stays fast (slow *business* behavior only).
* **error**  - business requests and the health probe answer 500.
* **status** - business requests answer the configured ``status`` (default
               500); the health probe answers that same status when >= 500
               (so a 5xx instance still trips), 200 otherwise.

Stopping the whole process (SIGTERM / kill) is the real "instance down"
case: the next connect is refused, exactly like a network failure.
"""

from __future__ import annotations

import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

CONTROL_PATH_PREFIX = "/__"


class UpstreamState:
    """Mutable per-process behavior plus the served-request counter."""

    def __init__(self, instance_id: str) -> None:
        self.instance_id = instance_id
        self._lock = threading.Lock()
        self.mode = "normal"
        self.delay = 0.0
        self.status = 500
        self.served_count = 0

    def set_mode(self, mode: str, delay: float = 0.0, status: int = 500) -> None:
        if mode not in ("normal", "slow", "error", "status"):
            raise ValueError(f"unknown mode: {mode!r}")
        with self._lock:
            self.mode = mode
            self.delay = float(delay)
            self.status = int(status)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "instance_id": self.instance_id,
                "mode": self.mode,
                "delay": self.delay,
                "status": self.status,
                "served_count": self.served_count,
            }

    def business_outcome(self) -> tuple[str, float, int]:
        with self._lock:
            return self.mode, self.delay, self.status

    def count_request(self) -> int:
        with self._lock:
            self.served_count += 1
            return self.served_count


def _make_handler(state: UpstreamState):
    class _Handler(BaseHTTPRequestHandler):
        server_version = "lab-upstream/1.0"

        def log_message(self, fmt, *args):  # quiet by default
            return

        # -- helpers -------------------------------------------------------

        def _send_json(self, status: int, payload: dict) -> None:
            raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("content-type", "application/json; charset=utf-8")
            self.send_header("content-length", str(len(raw)))
            self.send_header("x-upstream-instance", state.instance_id)
            self.end_headers()
            self.wfile.write(raw)

        def _read_body(self) -> bytes:
            length = int(self.headers.get("content-length", 0) or 0)
            return self.rfile.read(length) if length else b""

        def _echo_payload(self, body: bytes) -> dict:
            # Headers are passed through lowercased to match what the
            # gateway normalized; content-length reflects the *original*
            # request and is intentionally not echoed back as a claim.
            return {
                "instance_id": state.instance_id,
                "method": self.command,
                "path": self.path,
                "headers": {k.lower(): v for k, v in self.headers.items()},
                "body": body.decode("utf-8", errors="replace"),
                "body_bytes": len(body),
            }

        # -- control / probe endpoints ------------------------------------

        def _handle_control(self) -> bool:
            parsed = self.path.split("?", 1)[0]
            if parsed == "/__health":
                mode, _delay, status = state.business_outcome()
                if mode == "error" or (mode == "status" and status >= 500):
                    self._send_json(status, {"ok": False, "instance_id": state.instance_id})
                else:
                    self._send_json(200, {"ok": True, "instance_id": state.instance_id})
                return True
            if parsed == "/__meta":
                self._send_json(200, state.snapshot())
                return True
            if parsed == "/__control/state":
                self._send_json(200, state.snapshot())
                return True
            # POST /__control/mode is handled in do_POST.
            return parsed == "/__control/mode"

        # -- business entry, shared by every method -----------------------

        def _serve_business(self) -> None:
            body = self._read_body()
            mode, delay, status = state.business_outcome()
            if mode == "slow" and delay > 0:
                time.sleep(delay)
            state.count_request()
            if mode == "error":
                self._send_json(
                    500, {"error": "simulated upstream error", **self._echo_payload(body)}
                )
            elif mode == "status":
                self._send_json(status, self._echo_payload(body))
            else:
                self._send_json(200, self._echo_payload(body))

        def _dispatch(self) -> None:
            path_only = self.path.split("?", 1)[0]
            if path_only.startswith(CONTROL_PATH_PREFIX):
                if self._handle_control():
                    return
                self._send_json(404, {"error": "unknown control path"})
                return
            self._serve_business()

        def do_GET(self):
            self._dispatch()

        def do_POST(self):
            path_only = self.path.split("?", 1)[0]
            if path_only == "/__control/mode":
                try:
                    payload = json.loads(self._read_body() or b"{}")
                    state.set_mode(
                        payload.get("mode", "normal"),
                        delay=payload.get("delay", 0.0),
                        status=payload.get("status", 500),
                    )
                except (ValueError, json.JSONDecodeError) as exc:
                    self._send_json(400, {"ok": False, "error": str(exc)})
                    return
                self._send_json(200, {"ok": True, **state.snapshot()})
                return
            self._dispatch()

        do_PUT = _dispatch
        do_PATCH = _dispatch
        do_DELETE = _dispatch
        do_HEAD = _dispatch
        do_OPTIONS = _dispatch

    return _Handler


def create_server(port: int, instance_id: str, host: str = "127.0.0.1") -> ThreadingHTTPServer:
    """Build (not yet serve) the HTTP server bound to host:port."""

    state = UpstreamState(instance_id)
    server = ThreadingHTTPServer((host, port), _make_handler(state))
    server.upstream_state = state  # type: ignore[attr-defined]
    return server


def serve_forever(server: ThreadingHTTPServer) -> None:
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server.server_close()


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) < 2:
        print("usage: python -m edge_gateway.lab.upstream_server INSTANCE_ID PORT",
              file=sys.stderr)
        return 2
    instance_id, port = args[0], int(args[1])
    server = create_server(port, instance_id)
    print(f"upstream {instance_id} listening on 127.0.0.1:{port}", flush=True)
    try:
        serve_forever(server)
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
