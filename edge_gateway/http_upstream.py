"""Real network upstreams: the gateway talks HTTP over loopback to upstream
processes that can be started and stopped independently.

Connection refused (process stopped / restarting) maps to
:class:`UpstreamUnreachable` (the handler never ran; failover is safe).
A slow response past the timeout maps to :class:`UpstreamTimeout`
(execution is unconfirmed; non-idempotent requests are never retried).
Any other transport failure maps to :class:`UpstreamError`.

HTTP method, path, raw query string, body and common request headers are
forwarded verbatim (hop-by-hop headers are stripped). The upstream status
code, response body and response headers are returned unchanged to the
caller.
"""

from __future__ import annotations

import threading
from urllib.parse import urlencode

import httpx

from edge_gateway.models import RequestContext
from edge_gateway.upstream import UpstreamError, UpstreamTimeout, UpstreamUnreachable

# Hop-by-hop / transport headers must not be forwarded verbatim.
HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
        "host",
        "content-length",
    }
)


def _forward_headers(headers) -> dict[str, str]:
    return {k: v for k, v in headers.items() if k.lower() not in HOP_BY_HOP}


class HttpUpstreamRegistry:
    """instance_id -> base URL; every call is a real HTTP request."""

    def __init__(
        self,
        base_urls: dict[str, str],
        timeout: float = 2.0,
        client: httpx.Client | None = None,
    ) -> None:
        self._base_urls = dict(base_urls)
        self._timeout = timeout
        self._client = client or httpx.Client(timeout=timeout)
        self._owns_client = client is None
        self._lock = threading.Lock()

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def base_url(self, instance_id: str) -> str:
        return self._base_urls[instance_id]

    def set_base_url(self, instance_id: str, url: str) -> None:
        with self._lock:
            self._base_urls[instance_id] = url

    def _url_for(self, instance_id: str, request: RequestContext) -> str:
        base = self._base_urls[instance_id].rstrip("/")
        path = request.path if request.path.startswith("/") else "/" + request.path
        if request.query:
            query = urlencode(list(request.query.items()))
            return f"{base}{path}?{query}"
        return f"{base}{path}"

    def send(
        self, instance_id: str, request: RequestContext
    ) -> tuple[int, bytes, dict]:
        """Forward one request; return ``(status, raw_body, headers)``."""

        url = self._url_for(instance_id, request)
        try:
            resp = self._client.request(
                request.method,
                url,
                headers=_forward_headers(request.headers),
                content=request.body,
            )
        except httpx.ConnectError as exc:
            # Process stopped / still restarting: connection refused.
            raise UpstreamUnreachable(f"{instance_id} unreachable: {exc}") from exc
        except httpx.TimeoutException as exc:
            # Execution is unconfirmed: the caller must assume it may have run.
            raise UpstreamTimeout(f"{instance_id} timed out: {exc}") from exc
        except httpx.TransportError as exc:
            raise UpstreamError(f"{instance_id} transport error: {exc}") from exc

        return resp.status_code, resp.content, dict(resp.headers)

    def probe(self, instance_id: str) -> None:
        """Probe via ``GET /__health``.

        Raises :class:`UpstreamUnreachable` / :class:`UpstreamTimeout` on
        connection problems and :class:`UpstreamError` when the instance
        answers but is not healthy (>= 500). Probes never hit a business
        handler, so they cannot inflate execution counts.
        """

        url = self._base_urls[instance_id].rstrip("/") + "/__health"
        try:
            resp = self._client.get(url)
        except httpx.ConnectError as exc:
            raise UpstreamUnreachable(f"{instance_id} unreachable: {exc}") from exc
        except httpx.TimeoutException as exc:
            raise UpstreamTimeout(f"{instance_id} timed out: {exc}") from exc
        except httpx.TransportError as exc:
            raise UpstreamError(f"{instance_id} transport error: {exc}") from exc
        if resp.status_code >= 500:
            raise UpstreamError(f"{instance_id} probe answered HTTP {resp.status_code}")


__all__ = [
    "HOP_BY_HOP",
    "HttpUpstreamRegistry",
    "UpstreamError",
    "UpstreamTimeout",
    "UpstreamUnreachable",
]
