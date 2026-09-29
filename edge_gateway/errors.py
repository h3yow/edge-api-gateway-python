"""Gateway error types with stable, distinguishable error codes."""

from __future__ import annotations


class GatewayError(Exception):
    """Base class for all gateway-level errors."""

    code: str = "GATEWAY_ERROR"
    status_code: int = 500

    def __init__(self, message: str = "", details: dict | None = None) -> None:
        super().__init__(message or self.code)
        self.message = message or self.code
        self.details = details or {}


class NoRouteError(GatewayError):
    code = "NO_ROUTE"
    status_code = 404


class RouteConflictError(GatewayError):
    code = "ROUTE_CONFLICT"
    status_code = 409


class QuotaExceededError(GatewayError):
    code = "QUOTA_EXCEEDED"
    status_code = 429


class RateLimitExceededError(GatewayError):
    """Rejected because the caller is firing too fast (short-window burst)."""

    code = "RATE_LIMIT_EXCEEDED"
    status_code = 429


class InflightLimitExceededError(GatewayError):
    """Rejected because the caller already has too many requests in flight."""

    code = "INFLIGHT_LIMIT_EXCEEDED"
    status_code = 429


class UpstreamRequestError(GatewayError):
    """Transparent proxy failed to receive any usable upstream response."""

    code = "UPSTREAM_REQUEST_ERROR"
    status_code = 502


class AllUpstreamsUnavailableError(GatewayError):
    code = "ALL_UPSTREAMS_UNAVAILABLE"
    status_code = 503


class NonIdempotentRetryError(GatewayError):
    code = "NON_IDEMPOTENT_NOT_RETRIED"
    status_code = 502
