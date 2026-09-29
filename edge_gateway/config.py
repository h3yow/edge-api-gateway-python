"""Static gateway configuration: rules and upstream clusters.

Routing rules are evaluated by explicit numeric priority (higher wins).
Two rules at the *same* priority that both match a request are a conflict
and the gateway refuses to choose (see ``edge_gateway.router``).
"""

from __future__ import annotations

from edge_gateway.models import RouteRule


def default_rules() -> list[RouteRule]:
    """Return the demo rule set.

    Priorities are spaced so that a request can never match two rules at
    the same priority unintentionally; any future same-priority overlap is
    treated as ROUTE_CONFLICT rather than resolved by definition order.
    """

    return [
        # Explicit canary selection for a flagged caller.
        RouteRule(
            id="users-canary",
            priority=500,
            cluster="users-v2",
            path_prefix="/users",
            methods=frozenset({"GET"}),
            header_equals={"x-canary": "true"},
            quota_limit=10,
            quota_window_seconds=1.0,
        ),
        # Writes to /users go to the v1 cluster and are never retried.
        RouteRule(
            id="users-write",
            priority=400,
            cluster="users-v1",
            path_prefix="/users",
            methods=frozenset({"POST", "PUT", "PATCH", "DELETE"}),
            quota_limit=5,
            quota_window_seconds=1.0,
        ),
        # Reads of /users.
        RouteRule(
            id="users-read",
            priority=300,
            cluster="users-v1",
            path_prefix="/users",
            methods=frozenset({"GET"}),
            quota_limit=8,
            quota_window_seconds=1.0,
        ),
        # All three admission categories at once: short-burst rate,
        # simultaneous inflight, and a per-window total budget. Demonstrates
        # RATE_LIMITED / INFLIGHT_LIMIT_EXCEEDED / QUOTA_EXCEEDED separately.
        RouteRule(
            id="payments-api",
            priority=350,
            cluster="users-v1",
            path_prefix="/payments",
            quota_limit=5,
            quota_window_seconds=3.0,
            rate_limit=3,
            rate_window_seconds=1.0,
            inflight_limit=2,
        ),
        # Caller-scoped orders route, illustrating caller matching.
        RouteRule(
            id="orders-vip",
            priority=250,
            cluster="orders-v2",
            path_prefix="/orders",
            callers=frozenset({"vip"}),
            quota_limit=6,
            quota_window_seconds=1.0,
        ),
        RouteRule(
            id="orders-default",
            priority=200,
            cluster="orders-v1",
            path_prefix="/orders",
            quota_limit=4,
            quota_window_seconds=1.0,
        ),
        # Intentional conflicting pair, equal priority: requests to
        # /conflict with header x-experiment=A match both and must be
        # rejected with ROUTE_CONFLICT rather than silently picked.
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


def default_clusters() -> dict[str, list[str]]:
    """Map cluster name to ordered simulated instance ids.

    Order is deterministic; the gateway tries instances in this order and
    never picks randomly.
    """

    return {
        "users-v1": ["users-v1-1", "users-v1-2"],
        "users-v2": ["users-v2-1"],
        "orders-v1": ["orders-v1-1", "orders-v1-2"],
        "orders-v2": ["orders-v2-1"],
    }


#: Fixed (instance id -> (host, port)) topology for the local network drill.
#: Ports stay fixed so the drill CLI and the gateway share the same map and
#: individual upstream processes can be addressed directly.
DEFAULT_NETWORK_TOPOLOGY: dict[str, tuple[str, int]] = {
    "users-v1-1": ("127.0.0.1", 19101),
    "users-v1-2": ("127.0.0.1", 19102),
    "users-v2-1": ("127.0.0.1", 19103),
    "orders-v1-1": ("127.0.0.1", 19104),
    "orders-v1-2": ("127.0.0.1", 19105),
    "orders-v2-1": ("127.0.0.1", 19106),
}


def default_network_base_urls(
    topology: dict[str, tuple[str, int]] | None = None,
) -> dict[str, str]:
    topology = topology or DEFAULT_NETWORK_TOPOLOGY
    return {
        instance_id: f"http://{host}:{port}"
        for instance_id, (host, port) in topology.items()
    }
