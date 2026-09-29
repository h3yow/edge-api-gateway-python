"""One-command local drill orchestrator.

Starts every standalone upstream and the network gateway as real operating-
system processes (each upstream independently startable/stopable), tracks
their PIDs in a state directory, and drives per-instance behavior through
the upstream control HTTP API (stop process / slow response / error
response / custom status / back to normal).

Commands::

    python -m edge_gateway.lab.orchestrator up
    python -m edge_gateway.lab.orchestrator status
    python -m edge_gateway.lab.orchestrator stop users-v1-1
    python -m edge_gateway.lab.orchestrator start users-v1-1
    python -m edge_gateway.lab.orchestrator slow users-v1-1 1.5
    python -m edge_gateway.lab.orchestrator error users-v1-1
    python -m edge_gateway.lab.orchestrator respond users-v1-1 418
    python -m edge_gateway.lab.orchestrator normal users-v1-1
    python -m edge_gateway.lab.orchestrator down
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx

from edge_gateway.lab import lab_config

GATEWAY_MODULE = "main_net"
INSTANCE_IDS = list(lab_config.default_ports().keys())


class LabOrchestrator:
    def __init__(self, state_dir: str | None = None) -> None:
        root = Path(__file__).resolve().parents[2]
        self._root = root
        self._state_dir = Path(state_dir) if state_dir else root / ".lab_state"
        self._state_dir.mkdir(parents=True, exist_ok=True)
        self._ports = lab_config.default_ports()
        self._logs_dir = self._state_dir / "logs"
        self._logs_dir.mkdir(exist_ok=True)

    # -- pid bookkeeping ---------------------------------------------------

    def _pidfile(self, name: str) -> Path:
        return self._state_dir / f"{name}.pid"

    def _write_pid(self, name: str, proc: subprocess.Popen) -> None:
        self._pidfile(name).write_text(str(proc.pid))

    def _read_pid(self, name: str) -> int | None:
        path = self._pidfile(name)
        if not path.exists():
            return None
        try:
            return int(path.read_text().strip())
        except ValueError:
            return None

    def _clear_pid(self, name: str) -> None:
        self._pidfile(name).unlink(missing_ok=True)

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

    # -- process control ---------------------------------------------------

    def _start_upstream(self, instance_id: str) -> subprocess.Popen:
        port = self._ports[instance_id]
        log = open(self._logs_dir / f"{instance_id}.log", "ab")  # noqa: SIM115
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "edge_gateway.lab.upstream_server",
                instance_id,
                str(port),
            ],
            cwd=self._root,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        self._write_pid(instance_id, proc)
        return proc

    def _wait_http(self, url: str, timeout: float = 5.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                resp = httpx.get(url, timeout=0.5)
                if resp.status_code < 500:
                    return True
            except httpx.TransportError:
                pass
            time.sleep(0.05)
        return False

    def start_instance(self, instance_id: str, wait: bool = True) -> None:
        if instance_id not in self._ports:
            raise KeyError(f"unknown instance {instance_id!r}")
        if self._alive(self._read_pid(instance_id)):
            print(f"{instance_id} already running")
            return
        proc = self._start_upstream(instance_id)
        if wait:
            url = f"http://127.0.0.1:{self._ports[instance_id]}/__health"
            if not self._wait_http(url):
                raise RuntimeError(f"{instance_id} did not become ready")
        print(f"started {instance_id} (pid {proc.pid}) on port {self._ports[instance_id]}")

    def stop_instance(self, instance_id: str) -> None:
        pid = self._read_pid(instance_id)
        if not self._alive(pid):
            self._clear_pid(instance_id)
            print(f"{instance_id} was not running")
            return
        os.kill(pid, signal.SIGTERM)
        deadline = time.time() + 3.0
        while time.time() < deadline and self._alive(pid):
            time.sleep(0.05)
        if self._alive(pid):
            os.kill(pid, signal.SIGKILL)
        self._clear_pid(instance_id)
        print(f"stopped {instance_id} (pid {pid})")

    def _start_gateway(self, port: int) -> subprocess.Popen:
        log = open(self._logs_dir / "gateway.log", "ab")  # noqa: SIM115
        env = dict(os.environ)
        proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                f"{GATEWAY_MODULE}:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
            ],
            cwd=self._root,
            stdout=log,
            stderr=subprocess.STDOUT,
            env=env,
        )
        self._write_pid("gateway", proc)
        return proc

    def up(self, gateway_port: int = lab_config.DEFAULT_GATEWAY_PORT) -> None:
        for instance_id in INSTANCE_IDS:
            self.start_instance(instance_id)
        gw = self._start_gateway(gateway_port)
        if not self._wait_http(f"http://127.0.0.1:{gateway_port}/healthz"):
            raise RuntimeError("gateway did not become ready")
        print(f"started gateway (pid {gw.pid}) on http://127.0.0.1:{gateway_port}")
        print("lab is up")

    def down(self) -> None:
        pid = self._read_pid("gateway")
        if self._alive(pid):
            os.kill(pid, signal.SIGTERM)
            self._clear_pid("gateway")
        for instance_id in INSTANCE_IDS:
            self.stop_instance(instance_id)
        print("lab is down")

    # -- behavior control --------------------------------------------------

    def set_mode(
        self,
        instance_id: str,
        mode: str,
        delay: float = 0.0,
        status: int = 500,
    ) -> None:
        url = f"http://127.0.0.1:{self._ports[instance_id]}/__control/mode"
        resp = httpx.post(
            url, json={"mode": mode, "delay": delay, "status": status}, timeout=2.0
        )
        resp.raise_for_status()
        print(f"{instance_id} -> {resp.json()}")

    def status(self) -> dict:
        out = {"gateway": {}, "instances": {}}
        gw_pid = self._read_pid("gateway")
        out["gateway"] = {"pid": gw_pid, "alive": self._alive(gw_pid)}
        for instance_id in INSTANCE_IDS:
            entry = {"pid": self._read_pid(instance_id), "alive": False, "state": None}
            entry["alive"] = self._alive(entry["pid"])
            if entry["alive"]:
                try:
                    resp = httpx.get(
                        f"http://127.0.0.1:{self._ports[instance_id]}/__control/state",
                        timeout=1.0,
                    )
                    entry["state"] = resp.json()
                except httpx.TransportError:
                    entry["state"] = None
            out["instances"][instance_id] = entry
        return out

    def print_status(self) -> None:
        print(json.dumps(self.status(), indent=2, ensure_ascii=False))


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    orch = LabOrchestrator()
    if not args:
        print(__doc__)
        return 0
    cmd = args[0]
    rest = args[1:]
    try:
        if cmd == "up":
            orch.up(int(rest[0]) if rest else lab_config.DEFAULT_GATEWAY_PORT)
        elif cmd == "down":
            orch.down()
        elif cmd == "status":
            orch.print_status()
        elif cmd == "start":
            orch.start_instance(rest[0])
        elif cmd == "stop":
            orch.stop_instance(rest[0])
        elif cmd == "slow":
            orch.set_mode(rest[0], "slow", delay=float(rest[1]) if len(rest) > 1 else 1.0)
        elif cmd == "error":
            orch.set_mode(rest[0], "error")
        elif cmd == "normal":
            orch.set_mode(rest[0], "normal")
        elif cmd == "respond":
            orch.set_mode(rest[0], "status", status=int(rest[1]))
        else:
            print(f"unknown command: {cmd}", file=sys.stderr)
            return 2
    except (IndexError, KeyError, ValueError) as exc:
        print(f"invalid arguments: {exc}", file=sys.stderr)
        return 2
    except (httpx.HTTPError, RuntimeError) as exc:
        print(f"command failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
