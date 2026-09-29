"""Background recovery probing, decoupled from business traffic.

A :class:`BackgroundProber` runs its own daemon thread and periodically asks
the :class:`~edge_gateway.health.HealthMonitor` for due probes, so an instance
removed from rotation is brought back at its own cadence even when no user
requests arrive at all.

Concurrency with opportunistic on-traffic probes stays safe because probe
claiming is atomic inside the HealthMonitor: only one probing path (traffic
or background thread) ever claims a given instance at a time, so there are
neither duplicate probes nor OPEN/CLOSED flapping.
"""

from __future__ import annotations

import threading
from typing import Callable

from edge_gateway.health import HealthMonitor


class BackgroundProber:
    """Periodically execute due recovery probes on a daemon thread."""

    def __init__(
        self,
        health: HealthMonitor,
        interval: float = 0.02,
        sleep_func: Callable[[float], None] = __import__("time").sleep,
    ) -> None:
        self._health = health
        self._interval = interval
        self._sleep = sleep_func
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def run_once(self) -> int:
        """Claim and execute all currently-due probes. Returns probe count.

        Claiming is atomic per instance in the HealthMonitor, so this method
        is safe to call from the background thread while traffic flows.
        """

        claimed = self._health.due_probes()
        for instance_id in claimed:
            self._health.probe(instance_id)
        return len(claimed)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception:  # pragma: no cover - a probe thread must never die
                pass
            # Interruptible sleep so stop() returns promptly.
            self._stop.wait(self._interval)

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="background-prober", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 1.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=timeout)
        self._thread = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()
