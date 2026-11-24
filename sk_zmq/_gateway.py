"""Synchronous ZeroMQ request/reply transport."""

from __future__ import annotations

import logging
import threading
from collections.abc import Mapping
from typing import Any

import orjson
import zmq

from ._protocol import GatewayRequestCode, GatewayRequestResult

logger = logging.getLogger(__name__)

_MAX_BACKOFF_SECONDS = 30.0


class _InvalidGatewayResponseError(ValueError):
    """Raised when a transport response violates the gateway envelope."""


def _encode_request(request: Mapping[str, Any]) -> bytes | None:
    try:
        return orjson.dumps(request)
    except (TypeError, ValueError) as error:
        logger.error("Could not serialize gateway request: %s", error)
        return None


def _decode_response(raw_response: bytes) -> GatewayRequestResult:
    try:
        response = orjson.loads(raw_response)
    except (orjson.JSONDecodeError, TypeError) as error:
        raise _InvalidGatewayResponseError(
            "gateway response is not valid JSON"
        ) from error

    if not isinstance(response, dict):
        raise _InvalidGatewayResponseError(
            f"gateway response must be a JSON object, got {type(response).__name__}"
        )

    response_status = response.get("status")
    if response_status == "ok":
        return GatewayRequestResult.success(
            data=response.get("data"),
            response=response,
        )
    if response_status == "error":
        return GatewayRequestResult.failure(
            GatewayRequestCode.GATEWAY_REJECTED,
            response=response,
        )
    raise _InvalidGatewayResponseError(
        "gateway response status must be 'ok' or 'error'"
    )


def _send_once(
    *,
    context: zmq.Context,
    endpoint: str,
    encoded_request: bytes,
    response_timeout_ms: int,
) -> GatewayRequestResult:
    request_socket = context.socket(zmq.REQ)
    try:
        request_socket.setsockopt(zmq.RCVTIMEO, response_timeout_ms)
        request_socket.setsockopt(zmq.LINGER, 0)
        request_socket.connect(endpoint)
        request_socket.send(encoded_request)
        return _decode_response(request_socket.recv())
    finally:
        try:
            request_socket.close()
        except Exception:
            logger.exception("Could not close gateway request socket")


def _wait_before_retry(
    stop_requested: threading.Event,
    backoff_seconds: float,
) -> bool:
    logger.debug("Retrying gateway request in %.2f seconds", backoff_seconds)
    return stop_requested.wait(backoff_seconds)


def send_gateway_request(
    *,
    context: zmq.Context,
    stop_requested: threading.Event,
    host: str,
    port: int,
    request: Mapping[str, Any],
    max_attempts: int = 5,
    initial_backoff_seconds: float = 2,
    response_timeout_ms: int = 10_000,
) -> GatewayRequestResult:
    """Send one request, retrying timeouts and malformed transport responses."""
    if max_attempts <= 0:
        raise ValueError("max_attempts must be greater than 0")

    encoded_request = _encode_request(request)
    if encoded_request is None:
        return GatewayRequestResult.failure(GatewayRequestCode.INVALID_REQUEST)

    endpoint = f"tcp://{host}:{port}"
    backoff_seconds = initial_backoff_seconds
    last_error_code = GatewayRequestCode.TRANSPORT_ERROR

    for attempt_index in range(max_attempts):
        if attempt_index > 0 and stop_requested.is_set():
            logger.debug("Gateway retry canceled because shutdown was requested")
            return GatewayRequestResult.failure(GatewayRequestCode.CANCELED)

        try:
            return _send_once(
                context=context,
                endpoint=endpoint,
                encoded_request=encoded_request,
                response_timeout_ms=response_timeout_ms,
            )
        except zmq.Again:
            last_error_code = GatewayRequestCode.TIMEOUT
            logger.warning(
                "Gateway request timed out (attempt %d/%d)",
                attempt_index + 1,
                max_attempts,
            )
        except _InvalidGatewayResponseError as error:
            last_error_code = GatewayRequestCode.INVALID_RESPONSE
            logger.error(
                "Gateway response validation failed (attempt %d/%d): %s",
                attempt_index + 1,
                max_attempts,
                error,
            )
        except Exception as error:
            last_error_code = GatewayRequestCode.TRANSPORT_ERROR
            logger.error(
                "Gateway request failed (attempt %d/%d): %s",
                attempt_index + 1,
                max_attempts,
                error,
            )

        if stop_requested.is_set():
            return GatewayRequestResult.failure(GatewayRequestCode.CANCELED)

        has_more_attempts = attempt_index < max_attempts - 1
        if has_more_attempts and _wait_before_retry(stop_requested, backoff_seconds):
            return GatewayRequestResult.failure(GatewayRequestCode.CANCELED)
        backoff_seconds = min(backoff_seconds * 2, _MAX_BACKOFF_SECONDS)

    logger.error(
        "Gateway request failed after %d attempts (action=%r, interval=%r)",
        max_attempts,
        request.get("action"),
        request.get("interval"),
    )
    return GatewayRequestResult.failure(last_error_code)
