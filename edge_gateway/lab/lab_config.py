"""Static configuration for the local network drill environment."""

from __future__ import annotations

from edge_gateway.models import RouteRule

DEFAULT_GATEWAY_PORT = 8000

# Fixed loopback ports, spaced away from the gateway's 8000 and from the
# common 91xx range (which can be taken by other local services).
_DEFAULT_PORTS = {
    "users-v1-1": 9601,
    "users-v1-2": 9602,
    "users-v2-1": 9603,
    "orders-v1-1": 9604,
    "orders-v1-2": 9605,
    "orders-v2-1": 9606,
}


def default_ports() -> dict[str, int]:
    """instance_id -> loopback port."""

    return dict(_DEFAULT_PORTS)


def base_urls(ports: dict[str, int] | None = None) -> dict[str, str]:
    """instance_id -> http://127.0.0.1:port base URL."""

    ports = ports or default_ports()
    return {instance_id: f"http://127.0.0.1:{port}" for instance_id, port in ports.items()}


def default_clusters() -> dict[str, list[str]]:
    return {
        "users-v1": ["users-v1-1", "users-v1-2"],
        "users-v2": ["users-v2-1"],
        "orders-v1": ["orders-v1-1", "orders-v1-2"],
        "orders-v2": ["orders-v2-1"],
    }


def default_rules() -> list[RouteRule]:
    """Drill rule set.

    Same priorities and match dimensions as the demo rules, but every rule
    carries all three admission categories so the network drill exercises
    rate / inflight / quota independently:

    * ``rate_limit``  - max accepted in a short 0.2s burst window,
    * ``inflight_limit`` - max simultaneously in-flight requests,
    * ``quota_limit`` - max total accepted per 1s cycle.
    """

    return [
        RouteRule(
            id="users-canary",
            priority=500,
            cluster="users-v2",
            path_prefix="/users",
            methods=frozenset({"GET"}),
            header_equals={"x-canary": "true"},
            quota_limit=10,
            quota_window_seconds=1.0,
            rate_limit=10,
            inflight_limit=4,
        ),
        RouteRule(
            id="users-write",
            priority=400,
            cluster="users-v1",
            path_prefix="/users",
            methods=frozenset({"POST", "PUT", "PATCH", "DELETE"}),
            quota_limit=5,
            quota_window_seconds=1.0,
            rate_limit=5,
            inflight_limit=3,
        ),
        RouteRule(
            id="users-read",
            priority=300,
            cluster="users-v1",
            path_prefix="/users",
            methods=frozenset({"GET"}),
            quota_limit=8,
            quota_window_seconds=1.0,
            rate_limit=8,
            inflight_limit=4,
        ),
        RouteRule(
            id="orders-vip",
            priority=250,
            cluster="orders-v2",
            path_prefix="/orders",
            callers=frozenset({"vip"}),
            quota_limit=6,
            quota_window_seconds=1.0,
            rate_limit=6,
            inflight_limit=3,
        ),
        RouteRule(
            id="orders-default",
            priority=200,
            cluster="orders-v1",
            path_prefix="/orders",
            quota_limit=4,
            quota_window_seconds=1.0,
            rate_limit=4,
            inflight_limit=2,
        ),
        RouteRule(
            id="conflict-alpha",
            priority=100,
            cluster="users-v1",
            path_prefix="/conflict",
            header_equals={"x-experiment": "A"},
        ),
        RouteRule(
            id="conflict-beta",
            priority=100,
            cluster="orders-v1",
            path_prefix="/conflict",
            header_equals={"x-experiment": "A"},
        ),
    ]
