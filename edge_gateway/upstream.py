"""Simulated local upstream instances.

Instances are plain in-process callables, so the whole system runs locally
without external services. Every instance records how many times it actually
executed a request: this lets tests prove that a failed-over non-idempotent
request was *not* executed twice.

Faults are injectable per instance:
* ``fail``    - the handler "executes" and returns HTTP 500.
* ``timeout`` - the call raises :class:`UpstreamTimeout` before execution is
  confirmed; to be safe the gateway treats this as *not* confirmed and never
  retries non-idempotent methods.
* ``down``    - connection-level failure before the handler runs
  (:class:`UpstreamUnreachable`); retry is safe for every method.
"""

from __future__ import annotations

import json
import threading
from collections import defaultdict
from typing import Callable

Handler = Callable[[str, bytes], tuple[int, dict]]


class UpstreamError(Exception):
    """Base class for simulated transport failures."""


class UpstreamUnreachable(UpstreamError):
    """Connection refused / circuit open: handler never ran, retry is safe."""


class UpstreamTimeout(UpstreamError):
    """Timed out; execution is unconfirmed, non-idempotent calls are not retried."""


class UpstreamRegistry:
    """In-process registry of simulated upstream instances."""

    def __init__(self) -> None:
        self._handlers: dict[str, Handler] = {}
        self._clusters: dict[str, str] = {}
        self._faults: dict[str, str | None] = {}
        self._call_counts: dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    def register(self, instance_id: str, handler: Handler, cluster: str) -> None:
        with self._lock:
            self._handlers[instance_id] = handler
            self._clusters[instance_id] = cluster
            self._faults.setdefault(instance_id, None)

    def cluster_of(self, instance_id: str) -> str | None:
        return self._clusters.get(instance_id)

    def call(self, instance_id: str, body: bytes) -> tuple[int, dict]:
        with self._lock:
            handler = self._handlers.get(instance_id)
            fault = self._faults.get(instance_id)
        if handler is None:
            raise UpstreamUnreachable(f"unknown instance {instance_id!r}")

        if fault == "down":
            # Transport fails before the handler is entered.
            raise UpstreamUnreachable(f"{instance_id} is down")
        if fault == "timeout":
            # Timeout is unconfirmed: the caller must assume it may have run.
            raise UpstreamTimeout(f"{instance_id} timed out")

        # The handler is entered exactly once here.
        with self._lock:
            self._call_counts[instance_id] += 1
        status, payload = handler(instance_id, body)
        if fault == "fail":
            return 500, {"error": "simulated failure", "instance": instance_id}
        return status, payload

    def send(self, instance_id: str, request) -> tuple[int, bytes, dict]:
        """Transport-level call returning ``(status, raw_body, headers)``.

        This is the same transport surface as the real HTTP registry, so the
        gateway pipeline is identical for simulated and network upstreams.
        It delegates to :meth:`call` (which is where the user-request counter
        is incremented), keeping non-idempotent execution assertions exact.
        """

        status, payload = self.call(instance_id, request.body)
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers = {"content-type": "application/json; charset=utf-8"}
        return status, raw, headers

    def probe(self, instance_id: str) -> None:
        """Health-probe transport: raises UpstreamError when unreachable.

        Unlike :meth:`call` this never increments the user-request counter,
        so probes cannot muddy non-idempotent execution-count assertions.
        """

        with self._lock:
            handler = self._handlers.get(instance_id)
            fault = self._faults.get(instance_id)
        if handler is None:
            raise UpstreamUnreachable(f"unknown instance {instance_id!r}")
        if fault == "down":
            raise UpstreamUnreachable(f"{instance_id} is down")
        if fault == "timeout":
            raise UpstreamTimeout(f"{instance_id} timed out")
        # Healthy (or 'fail'): run the handler to exercise the path but do
        # not count it as a served user request.
        handler(instance_id, b"")
        if fault == "fail":
            raise UpstreamError(f"{instance_id} responds 500")

    def set_fault(self, instance_id: str, fault: str | None) -> None:
        if fault not in (None, "fail", "timeout", "down"):
            raise ValueError(f"unknown fault: {fault!r}")
        with self._lock:
            if instance_id not in self._handlers:
                raise KeyError(instance_id)
            self._faults[instance_id] = fault

    def fault_of(self, instance_id: str) -> str | None:
        return self._faults.get(instance_id)

    def call_count(self, instance_id: str) -> int:
        with self._lock:
            return self._call_counts.get(instance_id, 0)

    def reset_counters(self) -> None:
        with self._lock:
            self._call_counts.clear()

    def reset_all(self) -> None:
        with self._lock:
            for instance_id in self._faults:
                self._faults[instance_id] = None
            self._call_counts.clear()


def _echo_handler(instance_id: str, body: bytes) -> tuple[int, dict]:
    return 200, {"served_by": instance_id, "body_bytes": len(body)}


def build_default_registry(clusters: dict[str, list[str]]) -> UpstreamRegistry:
    registry = UpstreamRegistry()
    for cluster, instances in clusters.items():
        for instance_id in instances:
            registry.register(instance_id, _echo_handler, cluster)
    return registry
