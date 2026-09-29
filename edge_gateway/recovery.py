"""Background health recovery loop, decoupled from business traffic.

A daemon thread periodically asks the :class:`HealthMonitor` for due probes
and executes them. Consequently an instance that was removed from rotation
gets probed, and - after ``recovery_threshold`` consecutive successful
probes - restored **even when the gateway receives zero user traffic**.
The monitor's atomic claim guarantees the background loop and any
opportunistic, traffic-triggered probes never probe the same instance twice
nor fight over its state.
"""

from __future__ import annotations

import threading
from typing import Callable

from edge_gateway.health import HealthMonitor


class RecoveryLoop:
    def __init__(
        self,
        health: HealthMonitor,
        interval: float = 0.05,
        sleep_func: Callable[[float], None] | None = None,
    ) -> None:
        self._health = health
        self._interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._sleep = sleep_func or self._stop.wait

    def run_once(self) -> list[str]:
        """Atomically claim all due probes and execute them. Returns traces."""

        traces: list[str] = []
        for instance_id in self._health.due_probes():
            reopened = self._health.probe(instance_id)
            traces.append(
                f"recovery: background probe of {instance_id} ->"
                f" {'RECOVERED' if reopened else 'still out'}"
            )
        return traces

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception:  # pragma: no cover - a probe loop must not die
                pass
            # Interruptible sleep so stop() returns promptly.
            self._sleep(self._interval)

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="health-recovery", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None
