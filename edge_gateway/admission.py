"""Per-caller, per-rule admission control with three independent limits.

Categories
----------
* **rate** ("短时间冲得太猛"): at most ``rate_limit`` requests may *start*
  in each short ``rate_window_seconds`` burst window. Reject reason
  RATE_LIMITED.
* **inflight** ("同时在途太多"): at most ``inflight_limit`` requests for the
  same (caller, rule) may be executing simultaneously. The slot is acquired
  atomically with the other categories and released by :meth:`release` once
  the response (or an upstream error) comes back. Reject reason
  INFLIGHT_LIMIT_EXCEEDED.
* **quota** ("周期总额度用光"): at most ``quota_limit`` units per long
  ``quota_window_seconds``; units are never refunded. Reject reason
  QUOTA_EXCEEDED.

Atomicity
---------
All three categories share one lock and are decided in a single critical
section: first every category is evaluated against a fresh window state,
then, only if *all* pass, rate/inflight/quota are all consumed together.
Consequently a rejected request consumes nothing in *any* category, and
concurrency - even with slow or restarting upstreams - can never over-admit
any single category.
"""

from __future__ import annotations

import threading
from typing import Callable

from edge_gateway.models import RequestContext, RouteRule

# Checked in this order so the reject reason is deterministic.
RATE = "rate"
INFLIGHT = "inflight"
QUOTA = "quota"
_CATEGORY_ORDER = (RATE, INFLIGHT, QUOTA)


class _Window:
    __slots__ = ("start", "used")

    def __init__(self, start: float) -> None:
        self.start = start
        self.used = 0


class _KeyState:
    __slots__ = ("rate_win", "quota_win", "inflight")

    def __init__(self, now: float) -> None:
        self.rate_win: _Window | None = None
        self.quota_win: _Window | None = None
        self.inflight = 0


class AdmissionManager:
    def __init__(
        self, time_func: Callable[[], float] = __import__("time").monotonic
    ) -> None:
        self._time = time_func
        self._states: dict[tuple[str, str, str], _KeyState] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _key(request: RequestContext, rule: RouteRule) -> tuple[str, str, str]:
        return (request.caller, rule.id, rule.cluster)

    # -- window helpers (caller must hold the lock) ------------------------

    @staticmethod
    def _roll(window: _Window | None, now: float, length: float) -> _Window:
        if window is None:
            return _Window(now)
        if now - window.start >= length:
            return _Window(window.start + length)
        return window

    def _snapshot(
        self, request: RequestContext, rule: RouteRule, st: _KeyState
    ) -> dict:
        rate_snap: dict = {
            "limit": rule.rate_limit,
            "used": st.rate_win.used if st.rate_win else 0,
            "remaining": (
                max(rule.rate_limit - (st.rate_win.used if st.rate_win else 0), 0)
                if rule.rate_limit is not None
                else None
            ),
            "window_seconds": rule.rate_window_seconds,
        }
        if st.rate_win is not None and rule.rate_limit is not None:
            rate_snap["window_start"] = st.rate_win.start
            rate_snap["window_end"] = st.rate_win.start + rule.rate_window_seconds

        inflight_snap = {
            "limit": rule.inflight_limit,
            "inflight": st.inflight,
            "remaining": (
                max(rule.inflight_limit - st.inflight, 0)
                if rule.inflight_limit is not None
                else None
            ),
        }

        quota_snap = {
            "caller": request.caller,
            "rule_id": rule.id,
            "limit": rule.quota_limit,
            "used": st.quota_win.used if st.quota_win else 0,
            "remaining": (
                max(rule.quota_limit - (st.quota_win.used if st.quota_win else 0), 0)
                if rule.quota_limit is not None
                else None
            ),
            "window_seconds": rule.quota_window_seconds,
        }
        if st.quota_win is not None and rule.quota_limit is not None:
            quota_snap["window_start"] = st.quota_win.start
            quota_snap["window_end"] = st.quota_win.start + rule.quota_window_seconds

        return {
            "caller": request.caller,
            "rule_id": rule.id,
            RATE: rate_snap,
            INFLIGHT: inflight_snap,
            QUOTA: quota_snap,
        }

    # -- main decision -----------------------------------------------------

    def check(
        self, request: RequestContext, rule: RouteRule
    ) -> tuple[bool, dict, list[str]]:
        """Atomically evaluate and (only if all pass) consume the three slots.

        Returns ``(allowed, snapshot, trace)``. On rejection the snapshot's
        ``rejected_category`` names which limit fired (``rate`` /
        ``inflight`` / ``quota``) and every category reports its current
        state; nothing has been consumed.
        """

        trace: list[str] = []
        key = self._key(request, rule)
        now = self._time()
        with self._lock:
            st = self._states.get(key)
            if st is None:
                st = _KeyState(now)
                self._states[key] = st

            st.rate_win = self._roll(st.rate_win, now, rule.rate_window_seconds)
            st.quota_win = self._roll(st.quota_win, now, rule.quota_window_seconds)

            # Phase 1: evaluate every category without consuming anything.
            rate_used = st.rate_win.used
            quota_used = st.quota_win.used
            violations: dict[str, str] = {}
            if rule.rate_limit is not None and rate_used >= rule.rate_limit:
                violations[RATE] = (
                    f"{rate_used}/{rule.rate_limit} burst requests already in"
                    f" {rule.rate_window_seconds}s window"
                )
            if rule.inflight_limit is not None and st.inflight >= rule.inflight_limit:
                violations[INFLIGHT] = (
                    f"{st.inflight}/{rule.inflight_limit} requests already in flight"
                )
            if rule.quota_limit is not None and quota_used >= rule.quota_limit:
                violations[QUOTA] = (
                    f"{quota_used}/{rule.quota_limit} quota units used in"
                    f" {rule.quota_window_seconds}s window"
                )

            snapshot = self._snapshot(request, rule, st)
            if violations:
                category = next(c for c in _CATEGORY_ORDER if c in violations)
                snapshot["decision"] = "reject"
                snapshot["rejected_category"] = category
                trace.append(
                    f"admission: reject by {category} ({violations[category]});"
                    " no category consumed"
                )
                return False, snapshot, trace

            # Phase 2: all categories pass - consume them together.
            st.rate_win.used += 1
            st.quota_win.used += 1
            st.inflight += 1
            snapshot = self._snapshot(request, rule, st)
            snapshot["decision"] = "allow"
            trace.append(
                f"admission: allow rate={st.rate_win.used}/"
                f"{rule.rate_limit} inflight={st.inflight}/"
                f"{rule.inflight_limit} quota={st.quota_win.used}/"
                f"{rule.quota_limit}"
            )
            return True, snapshot, trace

    def release(self, request: RequestContext, rule: RouteRule) -> None:
        """Return one inflight slot after the request finished."""

        key = self._key(request, rule)
        with self._lock:
            st = self._states.get(key)
            if st is not None and st.inflight > 0:
                st.inflight -= 1

    def reset(self) -> None:
        with self._lock:
            self._states.clear()
