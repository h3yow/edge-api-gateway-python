"""Core data structures shared by routing, quota, health and dispatch."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping


@dataclass(frozen=True)
class AdmissionPolicy:
    """Three independent per-caller admission limits for one rule.

    Any field set to ``None`` means that category is unlimited.

    * ``rate_limit`` / ``rate_window_seconds`` - short-burst rate: at most
      this many requests may start within the window.
    * ``inflight_limit`` - number of requests that may be executing at the
      same time (released when the response comes back).
    * ``quota_limit`` / ``quota_window_seconds`` - total budget consumed per
      long window (never released; rolls over with the window).
    """

    rate_limit: int | None = None
    rate_window_seconds: float = 1.0
    inflight_limit: int | None = None
    quota_limit: int | None = None
    quota_window_seconds: float = 1.0


@dataclass(frozen=True)
class RouteRule:
    """A single routing rule. All set conditions must match (AND semantics)."""

    id: str
    priority: int
    cluster: str
    path_prefix: str | None = None
    methods: frozenset[str] | None = None
    header_equals: Mapping[str, str] = field(default_factory=dict)
    query_equals: Mapping[str, str] = field(default_factory=dict)
    callers: frozenset[str] | None = None
    quota_limit: int | None = None
    quota_window_seconds: float = 1.0
    rate_limit: int | None = None
    rate_window_seconds: float = 1.0
    inflight_limit: int | None = None

    def admission_policy(self) -> AdmissionPolicy:
        return AdmissionPolicy(
            rate_limit=self.rate_limit,
            rate_window_seconds=self.rate_window_seconds,
            inflight_limit=self.inflight_limit,
            quota_limit=self.quota_limit,
            quota_window_seconds=self.quota_window_seconds,
        )


@dataclass(frozen=True)
class RequestContext:
    """Normalized request representation, independent of the entry point."""

    method: str
    path: str
    headers: Mapping[str, str]
    query: Mapping[str, str]
    caller: str
    body: bytes = b""


@dataclass(frozen=True)
class AttemptRecord:
    """One upstream dispatch attempt, kept for explainability."""

    instance_id: str
    outcome: str
    detail: str = ""
    retried: bool = False


@dataclass
class ForwardResult:
    status_code: int = 0
    rule_id: str = ""
    cluster: str = ""
    instance_id: str = ""
    upstream_status: int = 0
    upstream_body: dict = field(default_factory=dict)
    attempts: list[AttemptRecord] = field(default_factory=list)
    trace: list[str] = field(default_factory=list)
    quota: dict = field(default_factory=dict)
    admission: dict = field(default_factory=dict)
    # Raw network transport: when set, the ASGI layer forwards these bytes
    # (and headers) verbatim instead of wrapping the dict in an envelope.
    raw_body: bytes | None = None
    raw_headers: dict = field(default_factory=dict)

    @property
    def transparent(self) -> bool:
        return self.raw_body is not None
