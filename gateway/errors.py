"""网关统一错误类型。

所有可预期的失败都抛出 :class:`GatewayError`，携带稳定的错误码 ``code``、
面向调用方的消息 ``message`` 以及用于解释判定依据的 ``details``。

错误码与 HTTP 状态码一一映射，调用方可以稳定地区分：
无匹配规则、规则冲突、调用方未知、限流、配额耗尽、并发限制、
上游全部不可用、上游错误等不同情形。
"""

from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    """网关错误码（字符串形式，直接出现在响应体中）。"""

    ROUTE_NO_MATCH = "ROUTE_NO_MATCH"
    ROUTE_AMBIGUOUS = "ROUTE_AMBIGUOUS"
    CALLER_UNIDENTIFIED = "CALLER_UNIDENTIFIED"
    RATE_LIMITED = "RATE_LIMITED"
    QUOTA_EXHAUSTED = "QUOTA_EXHAUSTED"
    CONCURRENCY_LIMITED = "CONCURRENCY_LIMITED"
    UPSTREAM_UNAVAILABLE = "UPSTREAM_UNAVAILABLE"
    UPSTREAM_ERROR = "UPSTREAM_ERROR"
    UPSTREAM_TIMEOUT = "UPSTREAM_TIMEOUT"
    INTERNAL_ERROR = "INTERNAL_ERROR"


#: 错误码到 HTTP 状态码的确定映射
HTTP_STATUS: dict[ErrorCode, int] = {
    ErrorCode.ROUTE_NO_MATCH: 404,
    ErrorCode.ROUTE_AMBIGUOUS: 409,
    ErrorCode.CALLER_UNIDENTIFIED: 401,
    ErrorCode.RATE_LIMITED: 429,
    ErrorCode.QUOTA_EXHAUSTED: 403,
    ErrorCode.CONCURRENCY_LIMITED: 429,
    ErrorCode.UPSTREAM_UNAVAILABLE: 503,
    ErrorCode.UPSTREAM_ERROR: 502,
    ErrorCode.UPSTREAM_TIMEOUT: 504,
    ErrorCode.INTERNAL_ERROR: 500,
}


class GatewayError(Exception):
    """所有网关错误的基类。

    :param code: 稳定错误码
    :param message: 简短人类可读说明
    :param details: 判定依据明细（必须可 JSON 序列化），用于“可解释”
    """

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        details: dict | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details: dict = details or {}

    @property
    def http_status(self) -> int:
        return HTTP_STATUS[self.code]

    def to_payload(self) -> dict:
        """转换为统一错误响应体。"""
        return {
            "error": {
                "code": self.code.value,
                "message": self.message,
                "details": self.details,
            }
        }


def no_match(method: str, path: str, evaluated: int) -> GatewayError:
    return GatewayError(
        ErrorCode.ROUTE_NO_MATCH,
        f"no route rule matches {method} {path}",
        {"method": method, "path": path, "rules_evaluated": evaluated},
    )


def ambiguous(method: str, path: str, rule_ids: list[str]) -> GatewayError:
    return GatewayError(
        ErrorCode.ROUTE_AMBIGUOUS,
        f"{len(rule_ids)} rules equally match {method} {path}",
        {"method": method, "path": path, "conflicting_rule_ids": rule_ids},
    )


def caller_unidentified() -> GatewayError:
    return GatewayError(
        ErrorCode.CALLER_UNIDENTIFIED,
        "missing or unknown caller identity header 'X-Caller-Id'",
        {"required_header": "X-Caller-Id"},
    )


def rate_limited(caller: str, retry_after: float, scope: str) -> GatewayError:
    return GatewayError(
        ErrorCode.RATE_LIMITED,
        f"caller '{caller}' exceeds request rate",
        {"caller_id": caller, "retry_after_seconds": round(retry_after, 4), "scope": scope},
    )


def quota_exhausted(caller: str, used: int, limit: int, reset_at: float) -> GatewayError:
    return GatewayError(
        ErrorCode.QUOTA_EXHAUSTED,
        f"caller '{caller}' quota exhausted",
        {
            "caller_id": caller,
            "used": used,
            "limit": limit,
            "window_reset_at": round(reset_at, 4),
        },
    )


def concurrency_limited(caller: str, in_flight: int, limit: int) -> GatewayError:
    return GatewayError(
        ErrorCode.CONCURRENCY_LIMITED,
        f"caller '{caller}' exceeds concurrent in-flight limit",
        {"caller_id": caller, "in_flight": in_flight, "limit": limit},
    )


def upstream_unavailable(rule_id: str, tried: list[str], healthy: list[str]) -> GatewayError:
    return GatewayError(
        ErrorCode.UPSTREAM_UNAVAILABLE,
        f"no available upstream for rule '{rule_id}'",
        {
            "rule_id": rule_id,
            "tried_instances": tried,
            "healthy_instances": healthy,
        },
    )


def upstream_error(message: str, details: dict) -> GatewayError:
    return GatewayError(ErrorCode.UPSTREAM_ERROR, message, details)


def upstream_timeout(message: str, details: dict) -> GatewayError:
    return GatewayError(ErrorCode.UPSTREAM_TIMEOUT, message, details)
