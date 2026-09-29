"""Per-caller, per-rule fixed-window quota enforcement.

Semantics
---------
* A quota of ``N`` units per window means the first ``N`` requests arriving
  in a window are **accepted** and each consumes exactly one unit; request
  ``N+1`` and later are rejected with QUOTA_EXCEEDED.
* Check-and-consume is a single critical section, so under concurrency the
  number of accepted requests never exceeds ``N`` even when upstreams are
  slow (the unit is taken before the upstream is called).
* Rejected requests never reach dispatch, so they consume no upstream
  resource.
* Windows roll over by wall/monotonic time. Rules without ``quota_limit``
  are unlimited.
"""

from __future__ import annotations

import threading
from typing import Callable

from edge_gateway.models import RequestContext, RouteRule


class QuotaManager:
    def __init__(self, time_func: Callable[[], float] = __import__("time").monotonic) -> None:
        self._time = time_func
        # key -> (window_start, used)
        self._windows: dict[tuple[str, str, str], tuple[float, int]] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _key(request: RequestContext, rule: RouteRule) -> tuple[str, str, str]:
        return (request.caller, rule.id, rule.cluster)

    def check(self, request: RequestContext, rule: RouteRule) -> tuple[bool, dict, list[str], str | None]:
        """Atomically decide and consume one quota unit.

        Returns ``(allowed, snapshot, trace, rejected_category)``. This is
        the same protocol as :class:`~edge_gateway.admission.AdmissionManager`;
        a plain QuotaManager only enforces the cycle-quota category and its
        ``release`` is a no-op (it tracks no in-flight count).

        ``snapshot`` always reports the state used for the decision (limit,
        used, remaining, window boundaries) so responses and logs can
        explain the verdict.
        """

        trace: list[str] = []
        if rule.quota_limit is None:
            snapshot = {
                "caller": request.caller,
                "rule_id": rule.id,
                "limit": None,
                "used": 0,
                "remaining": None,
                "window_seconds": None,
            }
            trace.append(f"quota: rule {rule.id!r} has no limit -> allow")
            return True, snapshot, trace, None

        key = self._key(request, rule)
        window = rule.quota_window_seconds
        now = self._time()
        with self._lock:
            entry = self._windows.get(key)
            if entry is None or now - entry[0] >= window:
                window_start = now if entry is None else entry[0] + window
                entry = (window_start, 0)
                self._windows[key] = entry
                trace.append(
                    f"quota: opened new window for {key} at t={window_start:.6f}"
                )

            window_start, used = entry
            if used < rule.quota_limit:
                used += 1
                self._windows[key] = (window_start, used)
                allowed = True
                decision = f"{used}/{rule.quota_limit} used -> allow (consume 1)"
            else:
                allowed = False
                decision = f"{used}/{rule.quota_limit} used -> reject (quota exhausted)"

        remaining = rule.quota_limit - used
        snapshot = {
            "caller": request.caller,
            "rule_id": rule.id,
            "limit": rule.quota_limit,
            "used": used,
            "remaining": max(remaining, 0),
            "window_seconds": window,
            "window_start": window_start,
            "window_end": window_start + window,
        }
        trace.append(f"quota: {decision}")
        return allowed, snapshot, trace, ("quota" if not allowed else None)

    def release(self, request: RequestContext, rule: RouteRule) -> None:
        """No inflight tracking in the plain quota manager."""

    def reset(self) -> None:
        with self._lock:
            self._windows.clear()
