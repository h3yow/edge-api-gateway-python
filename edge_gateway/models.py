"""Core data structures shared by routing, quota, health and dispatch."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping


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
