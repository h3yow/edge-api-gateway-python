"""Shared fixtures and logging helpers.

Every test prints the input, the decision basis and the final choice so the
suite is self-explaining. A ``logging`` handler also mirrors these lines, so
both ``-s`` output and captured logs carry the same information.
"""

from __future__ import annotations

import logging
import sys

import pytest

from edge_gateway.config import default_clusters, default_rules
from edge_gateway.gateway import Gateway
from edge_gateway.health import HealthMonitor
from edge_gateway.models import RequestContext
from edge_gateway.admission import AdmissionManager
from edge_gateway.router import Router, RuleRegistry
from edge_gateway.upstream import build_default_registry

logger = logging.getLogger("gateway-tests")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[gateway-test] %(message)s"))
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


def log_case(title: str, request: RequestContext | None = None, **fields) -> None:
    if request is not None:
        fields["input"] = {
            "method": request.method,
            "path": request.path,
            "caller": request.caller,
            "headers": dict(request.headers),
            "query": dict(request.query),
            "body_bytes": len(request.body),
        }
    lines = [f"CASE {title}"]
    lines += [f"  {key}: {value}" for key, value in fields.items()]
    block = "\n".join(lines)
    print(f"\n{block}")
    logger.info(block)


def make_request(method="GET", path="/", caller="alice", headers=None, query=None, body=b""):
    return RequestContext(
        method=method.upper(),
        path=path,
        headers={k.lower(): v for k, v in (headers or {}).items()},
        query=dict(query or {}),
        caller=caller,
        body=body,
    )


class Harness:
    """Fresh gateway instance with tunable health parameters."""

    def __init__(self, failure_threshold=3, recovery_threshold=2, probe_interval=0.0):
        self.clusters = default_clusters()
        self.rules = default_rules()
        self.registry = build_default_registry(self.clusters)
        instances = [i for ids in self.clusters.values() for i in ids]
        self.health = HealthMonitor(
            instances,
            self.registry,
            failure_threshold=failure_threshold,
            recovery_threshold=recovery_threshold,
            probe_interval=probe_interval,
        )
        self.quotas = AdmissionManager()
        self.gateway = Gateway(
            router=Router(RuleRegistry(self.rules)),
            quotas=self.quotas,
            health=self.health,
            registry=self.registry,
            clusters=self.clusters,
        )

    def handle(self, *args, **kwargs):
        return self.gateway.handle(make_request(*args, **kwargs))


@pytest.fixture()
def harness():
    return Harness()
