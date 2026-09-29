"""Local drill environment: real upstream processes + the gateway.

Every upstream is an independent OS process running
``python -m edge_gateway.realupstream`` (see :mod:`edge_gateway.realupstream`),
started and stopped individually and reached over real TCP. A run directory
holds one JSON state file per process with its PID/port, so separate CLI
invocations can stop, slow down or break a specific upstream while the rest
of the environment keeps running.

Commands
--------
``up``                start all upstreams, wait until healthy, start gateway
``down``              stop gateway and every upstream, clean state files
``stop <id>``         kill one upstream (simulates a process crash)
``start <id>``        restart one upstream
``slow <id> [delay]`` make one upstream delay every response (default 1.0s)
``error <id>``        make one upstream answer 500
``normal <id>``       restore normal behavior
``status``            show gateway/upstream processes and health
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

from edge_gateway.realupstream import HttpUpstreamRegistry
from edge_gateway.upstream import UpstreamError

GATEWAY_PORT = 19080
_HOST = "127.0.0.1"

# Fixed drill topology (same clusters as the demo rules, one port each).
DRILL_TOPOLOGY: dict[str, tuple[str, int]] = {
    "users-v1-1": ("users-v1", 19101),
    "users-v1-2": ("users-v1", 19102),
    "users-v2-1": ("users-v2", 19103),
    "orders-v1-1": ("orders-v1", 19104),
    "orders-v1-2": ("orders-v1", 19105),
    "orders-v2-1": ("orders-v2", 19106),
}


def _port_open(host: str, port: int, timeout: float = 0.25) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        return sock.connect_ex((host, port)) == 0


class DrillEnvironment:
    def __init__(self, workdir: str = ".drill", python: str | None = None) -> None:
        self.dir = Path(workdir)
        self.dir.mkdir(parents=True, exist_ok=True)
        self._python = python or sys.executable
        self._registry = HttpUpstreamRegistry(request_timeout=10.0, probe_timeout=1.0)
        for instance_id, (_cluster, port) in DRILL_TOPOLOGY.items():
            self._registry.register(
                instance_id, f"http://{_HOST}:{port}", _cluster
            )

    # -- state files -------------------------------------------------------

    def _state_path(self, name: str) -> Path:
        return self.dir / f"{name}.json"

    def _write_state(self, name: str, data: dict) -> None:
        self._state_path(name).write_text(json.dumps(data, indent=2))

    def _read_state(self, name: str) -> dict | None:
        path = self._state_path(name)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text())
        except (ValueError, OSError):
            return None

    def _clear_state(self, name: str) -> None:
        try:
            self._state_path(name).unlink()
        except FileNotFoundError:
            pass

    # -- process primitives ------------------------------------------------

    @staticmethod
    def _alive(pid: int | None) -> bool:
        if not pid:
            return False
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    def _terminate(self, pid: int) -> None:
        try:
            os.kill(pid, 15)  # SIGTERM
        except ProcessLookupError:
            return
        for _ in range(50):
            if not self._alive(pid):
                return
            time.sleep(0.02)
        try:
            os.kill(pid, 9)
        except ProcessLookupError:
            pass

    def start_upstream(self, instance_id: str) -> dict:
        if instance_id not in DRILL_TOPOLOGY:
            raise KeyError(f"unknown upstream {instance_id!r}")
        cluster, port = DRILL_TOPOLOGY[instance_id]
        prior = self._read_state(instance_id)
        if prior and self._alive(prior.get("pid")):
            return prior
        log = open(self.dir / f"{instance_id}.log", "ab")  # noqa: SIM115
        proc = subprocess.Popen(
            [
                self._python,
                "-m",
                "edge_gateway.realupstream",
                "--id",
                instance_id,
                "--host",
                _HOST,
                "--port",
                str(port),
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        # The child owns its lifecycle; release the Popen handle but keep
        # the log fd open in this short-lived CLI process too.
        data = {
            "instance_id": instance_id,
            "cluster": cluster,
            "host": _HOST,
            "port": port,
            "pid": proc.pid,
        }
        self._write_state(instance_id, data)
        self._wait_healthy(instance_id, timeout=5.0)
        return data

    def stop_upstream(self, instance_id: str) -> None:
        data = self._read_state(instance_id)
        if data and self._alive(data.get("pid")):
            self._terminate(data["pid"])
        self._clear_state(instance_id)

    def _wait_healthy(self, instance_id: str, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                self._registry.probe(instance_id)
                return
            except UpstreamError:
                time.sleep(0.05)
        raise TimeoutError(f"{instance_id} did not become healthy within {timeout}s")

    # -- gateway process ---------------------------------------------------

    def _start_gateway(self) -> dict:
        prior = self._read_state("gateway")
        if prior and self._alive(prior.get("pid")):
            return prior
        env = dict(os.environ)
        env["EG_MODE"] = "network"
        env["EG_GATEWAY_PORT"] = str(GATEWAY_PORT)
        env["EG_DRILL_DIR"] = str(self.dir)
        log = open(self.dir / "gateway.log", "ab")  # noqa: SIM115
        proc = subprocess.Popen(
            [
                self._python,
                "-m",
                "uvicorn",
                "edge_gateway.app:network_app",
                "--host",
                _HOST,
                "--port",
                str(GATEWAY_PORT),
                "--log-level",
                "warning",
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
            env=env,
        )
        data = {"pid": proc.pid, "host": _HOST, "port": GATEWAY_PORT}
        self._write_state("gateway", data)
        # Verify that *our* gateway answers, not some unrelated process that
        # happens to hold the port.
        import http.client

        deadline = time.monotonic() + 5.0
        last_error = ""
        while time.monotonic() < deadline:
            try:
                conn = http.client.HTTPConnection(_HOST, GATEWAY_PORT, timeout=1.0)
                conn.request("GET", "/healthz")
                resp = conn.getresponse()
                resp.read()
                conn.close()
                if resp.status == 200:
                    return data
            except OSError as exc:
                last_error = str(exc)
            time.sleep(0.05)
        self._terminate(proc.pid)
        self._clear_state("gateway")
        raise TimeoutError(
            f"gateway did not answer /healthz on {_HOST}:{GATEWAY_PORT}: {last_error}"
        )

    def _stop_gateway(self) -> None:
        data = self._read_state("gateway")
        if data and self._alive(data.get("pid")):
            self._terminate(data["pid"])
        self._clear_state("gateway")

    # -- whole environment -------------------------------------------------

    def up(self) -> dict:
        upstreams = {}
        for instance_id in DRILL_TOPOLOGY:
            upstreams[instance_id] = self.start_upstream(instance_id)
        gateway = self._start_gateway()
        return {"gateway": gateway, "upstreams": upstreams}

    def down(self) -> None:
        self._stop_gateway()
        for instance_id in DRILL_TOPOLOGY:
            self.stop_upstream(instance_id)

    # -- fault injection over the network ----------------------------------

    def set_mode(self, instance_id: str, mode: str | None, delay: float = 1.0) -> dict:
        if instance_id not in DRILL_TOPOLOGY:
            raise KeyError(f"unknown upstream {instance_id!r}")
        return self._registry.set_remote_mode(
            instance_id, mode or "normal", delay=delay
        )

    def status(self) -> dict:
        result: dict = {"gateway": None, "upstreams": {}}
        gw = self._read_state("gateway")
        if gw:
            result["gateway"] = {
                **gw,
                "alive": self._alive(gw.get("pid")),
                "port_open": _port_open(_HOST, gw["port"]),
            }
        for instance_id, (_cluster, port) in DRILL_TOPOLOGY.items():
            entry = self._read_state(instance_id)
            alive = bool(entry) and self._alive(entry.get("pid"))
            info: dict = {
                "cluster": _cluster,
                "port": port,
                "started": alive,
                "port_open": _port_open(_HOST, port),
            }
            if alive:
                try:
                    info["remote"] = self._registry.remote_state(instance_id)
                    info["healthy"] = True
                except UpstreamError:
                    info["healthy"] = False
            result["upstreams"][instance_id] = info
        return result


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="edge-drill", description=__doc__)
    parser.add_argument("--workdir", default=".drill")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("up")
    sub.add_parser("down")
    sub.add_parser("status")
    p_stop = sub.add_parser("stop")
    p_stop.add_argument("instance_id")
    p_start = sub.add_parser("start")
    p_start.add_argument("instance_id")
    p_slow = sub.add_parser("slow")
    p_slow.add_argument("instance_id")
    p_slow.add_argument("delay", nargs="?", type=float, default=1.0)
    p_error = sub.add_parser("error")
    p_error.add_argument("instance_id")
    p_normal = sub.add_parser("normal")
    p_normal.add_argument("instance_id")

    args = parser.parse_args(argv)
    env = DrillEnvironment(workdir=args.workdir)

    if args.cmd == "up":
        info = env.up()
        print(json.dumps(info, indent=2))
        print(f"gateway ready at http://{_HOST}:{GATEWAY_PORT}")
    elif args.cmd == "down":
        env.down()
        print("all stopped")
    elif args.cmd == "status":
        print(json.dumps(env.status(), indent=2))
    elif args.cmd == "stop":
        env.stop_upstream(args.instance_id)
        print(f"stopped {args.instance_id}")
    elif args.cmd == "start":
        print(json.dumps(env.start_upstream(args.instance_id), indent=2))
    elif args.cmd == "slow":
        print(json.dumps(env.set_mode(args.instance_id, "slow", args.delay), indent=2))
    elif args.cmd == "error":
        print(json.dumps(env.set_mode(args.instance_id, "error"), indent=2))
    elif args.cmd == "normal":
        print(json.dumps(env.set_mode(args.instance_id, "normal"), indent=2))


if __name__ == "__main__":
    main()
