"""Fine-grained per-caller admission control.

Three independent categories per (caller, rule):

* **rate**     - short-window burst cap (fixed window).
* **inflight** - number of requests concurrently in flight at one instant.
* **quota**    - total units accepted per (longer) cycle window.

Semantics
---------
* The whole check is one critical section: all enforced categories are
  evaluated and, only if *every* category has room, one unit is consumed
  from each. A rejected request therefore consumes nothing - not from the
  category it failed, and not from the other categories either.
* Consumption happens before the upstream is touched, so concurrency, slow
  upstreams or an upstream restart can never let extra requests through.
* The inflight unit is returned via :meth:`release` once the request
  finishes (success, business error or transport failure alike).
* Each rejection reports its own category plus the live rate/inflight/quota
  state so the reason is self-explanatory.
"""

from __future__ import annotations

import threading
from typing import Callable

from edge_gateway.models import RequestContext, RouteRule

RATE = "rate"
INFLIGHT = "inflight"
QUOTA = "quota"


def _key(request: RequestContext, rule: RouteRule) -> tuple[str, str]:
    return (request.caller, rule.id)


class AdmissionManager:
    def __init__(self, time_func: Callable[[], float] = __import__("time").monotonic) -> None:
        self._time = time_func
        # key -> dict with counters/windows
        self._state: dict[tuple[str, str], dict] = {}
        self._lock = threading.Lock()

    def _entry(self, key: tuple[str, str]) -> dict:
        entry = self._state.get(key)
        if entry is None:
            entry = {
                "rate_start": None,
                "rate_used": 0,
                "inflight": 0,
                "quota_start": None,
                "quota_used": 0,
            }
            self._state[key] = entry
        return entry

    @staticmethod
    def _roll_window(entry: dict, field_start: str, field_used: str, now: float,
                     window: float) -> None:
        """Start a fresh fixed window once the current one has elapsed."""
        start = entry[field_start]
        if start is None:
            # First window for this key starts now.
            entry[field_start] = now
            entry[field_used] = 0
        elif now - start >= window:
            entry[field_start] = start + window
            entry[field_used] = 0

    def _snapshot(self, request: RequestContext, rule: RouteRule, entry: dict) -> dict:
        return {
            "caller": request.caller,
            "rule_id": rule.id,
            RATE: {
                "limit": rule.rate_limit,
                "used": entry["rate_used"],
                "remaining": None
                if rule.rate_limit is None
                else max(rule.rate_limit - entry["rate_used"], 0),
                "window_seconds": rule.rate_window_seconds,
                "window_start": entry["rate_start"],
                "window_end": entry["rate_start"] + rule.rate_window_seconds,
            },
            INFLIGHT: {
                "limit": rule.inflight_limit,
                "used": entry["inflight"],
                "remaining": None
                if rule.inflight_limit is None
                else max(rule.inflight_limit - entry["inflight"], 0),
            },
            QUOTA: {
                "limit": rule.quota_limit,
                "used": entry["quota_used"],
                "remaining": None
                if rule.quota_limit is None
                else max(rule.quota_limit - entry["quota_used"], 0),
                "window_seconds": rule.quota_window_seconds,
                "window_start": entry["quota_start"],
                "window_end": entry["quota_start"] + rule.quota_window_seconds,
            },
        }

    def check(
        self, request: RequestContext, rule: RouteRule
    ) -> tuple[bool, dict, list[str], str | None]:
        """Atomically decide and consume.

        Returns ``(allowed, snapshot, trace, rejected_category)``. When
        rejected, ``rejected_category`` is one of ``"rate"``, ``"inflight"``
        or ``"quota"`` and no counter anywhere has moved.
        """

        trace: list[str] = []
        key = _key(request, rule)
        now = self._time()
        with self._lock:
            entry = self._entry(key)
            self._roll_window(entry, "rate_start", "rate_used", now, rule.rate_window_seconds)
            self._roll_window(
                entry, "quota_start", "quota_used", now, rule.quota_window_seconds
            )

            if rule.rate_limit is not None and entry["rate_used"] >= rule.rate_limit:
                trace.append(
                    f"admission: rate burst {entry['rate_used']}/{rule.rate_limit}"
                    f" in {rule.rate_window_seconds}s -> reject (RATE_LIMIT_EXCEEDED)"
                )
                return False, self._snapshot(request, rule, entry), trace, RATE

            if (
                rule.inflight_limit is not None
                and entry["inflight"] >= rule.inflight_limit
            ):
                trace.append(
                    f"admission: inflight {entry['inflight']}/{rule.inflight_limit}"
                    " -> reject (INFLIGHT_LIMIT_EXCEEDED)"
                )
                return False, self._snapshot(request, rule, entry), trace, INFLIGHT

            if rule.quota_limit is not None and entry["quota_used"] >= rule.quota_limit:
                trace.append(
                    f"admission: cycle quota {entry['quota_used']}/{rule.quota_limit}"
                    f" in {rule.quota_window_seconds}s -> reject (QUOTA_EXCEEDED)"
                )
                return False, self._snapshot(request, rule, entry), trace, QUOTA

            # All enforced categories have room: consume one from each.
            if rule.rate_limit is not None:
                entry["rate_used"] += 1
            if rule.inflight_limit is not None:
                entry["inflight"] += 1
            if rule.quota_limit is not None:
                entry["quota_used"] += 1
            trace.append(
                "admission: room in every category -> accept and consume"
                f" (rate={entry['rate_used'] if rule.rate_limit is not None else '-'}"
                f", inflight={entry['inflight'] if rule.inflight_limit is not None else '-'}"
                f", quota={entry['quota_used'] if rule.quota_limit is not None else '-'})"
            )
            return True, self._snapshot(request, rule, entry), trace, None

    def release(self, request: RequestContext, rule: RouteRule) -> None:
        """Return the inflight unit after the request has finished."""

        key = _key(request, rule)
        with self._lock:
            entry = self._state.get(key)
            if entry is not None and entry["inflight"] > 0:
                entry["inflight"] -= 1

    def inflight_of(self, request: RequestContext, rule: RouteRule) -> int:
        with self._lock:
            entry = self._state.get(_key(request, rule))
            return entry["inflight"] if entry else 0

    def reset(self) -> None:
        with self._lock:
            self._state.clear()
