"""Per-instance health tracking, tripping and probing.

State machine for each instance::

    CLOSED (available)
      -- failure_threshold consecutive failures --> OPEN (removed from rotation)
    OPEN
      -- probe_interval elapsed, a probe is allowed once --> PROBING
    PROBING
      -- probe succeeds --> recovery success #1 recorded, stays unavailable
      -- recovery_threshold consecutive probe successes --> CLOSED
      -- any failed probe --> OPEN (probe streak reset, timer restarts)

Concurrency guarantees
-----------------------
* All state transitions happen under one lock, so concurrent failure
  reports trip an instance exactly once (no duplicate remove events) and
  concurrent probes cannot race an instance between OPEN and CLOSED.
* At most one probe is in flight per instance: ``due_probes`` atomically
  marks the probe as started. This prevents thundering-herd probes and the
  OPEN/CLOSED flapping they would cause.
* Only a genuinely healthy instance (``recovery_threshold`` consecutive
  successful probes) re-enters rotation; a single lucky probe is not
  enough, which prevents flapping on flaky upstreams.
"""

from __future__ import annotations

import threading
from collections import deque
from typing import Callable

from edge_gateway.models import HealthEvent
from edge_gateway.upstream import UpstreamError, UpstreamRegistry

CLOSED = "CLOSED"
OPEN = "OPEN"
PROBING = "PROBING"


class _InstanceState:
    __slots__ = (
        "consecutive_failures",
        "last_probe_ok",
        "last_recovered_at",
        "last_recovered_reason",
        "last_removed_at",
        "last_removed_reason",
        "opened_at",
        "probe_in_flight",
        "recovery_successes",
        "state",
    )

    def __init__(self) -> None:
        self.state = CLOSED
        self.consecutive_failures = 0
        self.recovery_successes = 0
        self.opened_at = 0.0
        self.probe_in_flight = False
        self.last_probe_ok: bool | None = None
        self.last_removed_at: float | None = None
        self.last_removed_reason: str | None = None
        self.last_recovered_at: float | None = None
        self.last_recovered_reason: str | None = None


class HealthMonitor:
    def __init__(
        self,
        instances: list[str],
        registry: UpstreamRegistry,
        failure_threshold: int = 3,
        recovery_threshold: int = 2,
        probe_interval: float = 0.05,
        time_func: Callable[[], float] = __import__("time").monotonic,
        history_per_instance: int = 20,
    ) -> None:
        self._registry = registry
        self._failure_threshold = failure_threshold
        self._recovery_threshold = recovery_threshold
        self._probe_interval = probe_interval
        self._time = time_func
        self._states = {instance_id: _InstanceState() for instance_id in instances}
        # Append-only removal/recovery audit log, newest last. The deque is
        # only ever mutated under ``_lock``; reads copy under the same lock.
        self._events: deque[HealthEvent] = deque(maxlen=history_per_instance * max(len(instances), 1))
        self._lock = threading.Lock()

    # -- queries -----------------------------------------------------------

    def is_available(self, instance_id: str) -> bool:
        with self._lock:
            return self._states[instance_id].state == CLOSED

    def available_in(self, instances: list[str]) -> list[str]:
        with self._lock:
            return [i for i in instances if self._states[i].state == CLOSED]

    def snapshot(self) -> dict:
        with self._lock:
            return {
                instance_id: {
                    "state": s.state,
                    "consecutive_failures": s.consecutive_failures,
                    "recovery_successes": s.recovery_successes,
                    "probe_in_flight": s.probe_in_flight,
                    "last_probe_ok": s.last_probe_ok,
                    "last_removed_at": s.last_removed_at,
                    "last_removed_reason": s.last_removed_reason,
                    "last_recovered_at": s.last_recovered_at,
                    "last_recovered_reason": s.last_recovered_reason,
                }
                for instance_id, s in self._states.items()
            }

    def events(self, instance_id: str | None = None) -> list[dict]:
        """Return removal/recovery records, newest last, with time and reason.

        Filtered to one instance when ``instance_id`` is given. Each record
        is ``{instance_id, kind, reason, timestamp}`` where kind is
        ``removed`` or ``recovered``.
        """

        with self._lock:
            items = list(self._events)
        out = [
            {
                "instance_id": e.instance_id,
                "kind": e.kind,
                "reason": e.reason,
                "timestamp": e.timestamp,
            }
            for e in items
            if instance_id is None or e.instance_id == instance_id
        ]
        return out

    # -- live-traffic feedback --------------------------------------------

    def record_success(self, instance_id: str) -> None:
        with self._lock:
            s = self._states[instance_id]
            s.consecutive_failures = 0
            # Success on real traffic while OPEN (e.g. request raced the
            # trip) does not auto-close: recovery stays probe-driven so the
            # reopen decision is made at exactly one place.

    def record_failure(self, instance_id: str, reason: str | None = None) -> list[str]:
        """Record one live-traffic failure and trip at the threshold.

        Returns trace lines. The trip happens at most once per OPEN stint:
        further failures while OPEN keep the instance OPEN without emitting
        duplicate removal events.
        """

        trace: list[str] = []
        with self._lock:
            s = self._states[instance_id]
            s.consecutive_failures += 1
            n = s.consecutive_failures
            trace.append(
                f"health: {instance_id} failure {n}/{self._failure_threshold}"
            )
            if s.state == CLOSED and n >= self._failure_threshold:
                why = reason or f"{n} consecutive failures"
                now = self._time()
                s.state = OPEN
                s.opened_at = now
                s.recovery_successes = 0
                s.probe_in_flight = False
                s.last_removed_at = now
                s.last_removed_reason = why
                self._events.append(
                    HealthEvent(instance_id, "removed", why, now)
                )
                trace.append(
                    f"health: {instance_id} tripped OPEN and removed from rotation"
                    f" ({why})"
                )
            elif s.state == OPEN:
                trace.append(f"health: {instance_id} already OPEN (no duplicate trip)")
        return trace

    # -- probing -----------------------------------------------------------

    def due_probes(self) -> list[str]:
        """Atomically claim due probes; returns instance ids now claimed.

        A probe is due for an OPEN instance once ``probe_interval`` has
        elapsed and no probe is in flight. Claiming sets ``probe_in_flight``
        inside the same critical section, so concurrent callers can never
        double-probe one instance.
        """

        now = self._time()
        claimed: list[str] = []
        with self._lock:
            for instance_id, s in self._states.items():
                if s.state != OPEN:
                    continue
                if s.probe_in_flight:
                    continue
                if now - s.opened_at < self._probe_interval:
                    continue
                s.probe_in_flight = True
                s.state = PROBING
                claimed.append(instance_id)
        return claimed

    def probe(self, instance_id: str) -> bool:
        """Execute one claimed probe and apply the result atomically.

        Returns True only when this probe brought the instance back to
        CLOSED; returns False while it stays out of rotation.
        """

        try:
            self._registry.probe(instance_id)
            ok = True
        except UpstreamError:
            ok = False

        reopened = False
        with self._lock:
            s = self._states[instance_id]
            s.probe_in_flight = False
            s.last_probe_ok = ok
            if ok:
                s.recovery_successes += 1
                if s.recovery_successes >= self._recovery_threshold:
                    why = f"{s.recovery_successes} consecutive probe successes"
                    now = self._time()
                    s.state = CLOSED
                    s.consecutive_failures = 0
                    s.last_recovered_at = now
                    s.last_recovered_reason = why
                    self._events.append(
                        HealthEvent(instance_id, "recovered", why, now)
                    )
                    reopened = True
                else:
                    # Not yet convinced: keep it out of rotation but allow
                    # another probe after the interval.
                    s.state = OPEN
                    s.opened_at = self._time()
            else:
                s.recovery_successes = 0
                s.state = OPEN
                s.opened_at = self._time()
        return reopened

    def reset(self) -> None:
        with self._lock:
            for s in self._states.values():
                s.state = CLOSED
                s.consecutive_failures = 0
                s.recovery_successes = 0
                s.opened_at = 0.0
                s.probe_in_flight = False
                s.last_probe_ok = None
                s.last_removed_at = None
                s.last_removed_reason = None
                s.last_recovered_at = None
                s.last_recovered_reason = None
            self._events.clear()
