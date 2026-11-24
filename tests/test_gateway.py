"""Synchronous gateway request transport behavior."""

from __future__ import annotations

import threading
from unittest.mock import MagicMock

import zmq

from sk_zmq._gateway import send_gateway_request
from sk_zmq._protocol import GatewayRequestCode


def _transport(*responses):
    context = MagicMock()
    socket = MagicMock()
    socket.recv.side_effect = responses
    context.socket.return_value = socket
    return context, socket


def _send(context, request, **overrides):
    options = {
        "context": context,
        "stop_requested": threading.Event(),
        "host": "127.0.0.1",
        "port": 11556,
        "request": request,
        "initial_backoff_seconds": 0,
    }
    options.update(overrides)
    return send_gateway_request(**options)


def test_success_response_is_decoded_without_retry():
    context, socket = _transport(
        b'{"status":"ok","data":[1],"stream_id":"runtime","snapshot_seq":3}'
    )

    result = _send(context, {"action": "subscribe_candle"})

    assert result.ok
    assert result.data == [1]
    assert result.response["snapshot_seq"] == 3
    context.socket.assert_called_once_with(zmq.REQ)
    socket.close.assert_called_once_with()


def test_gateway_error_is_returned_without_retry():
    context, socket = _transport(b'{"status":"error","message":"denied"}')

    result = _send(context, {"action": "subscribe_candle"})

    assert result.code is GatewayRequestCode.GATEWAY_REJECTED
    assert result.response["message"] == "denied"
    assert socket.send.call_count == 1


def test_non_serializable_request_is_rejected_before_socket_allocation():
    context = MagicMock()

    result = _send(context, {"value": object()})

    assert result.code is GatewayRequestCode.INVALID_REQUEST
    context.socket.assert_not_called()


def test_invalid_response_envelope_retries_then_reports_invalid_response():
    context, socket = _transport(b"[]", b'{"status":"unknown"}')

    result = _send(
        context,
        {"action": "subscribe_candle"},
        max_attempts=2,
    )

    assert result.code is GatewayRequestCode.INVALID_RESPONSE
    assert socket.send.call_count == 2
    assert socket.close.call_count == 2


def test_transport_failure_retries_then_reports_transport_error():
    context, socket = _transport(
        RuntimeError("offline"),
        RuntimeError("still offline"),
    )

    result = _send(
        context,
        {"action": "subscribe_candle"},
        max_attempts=2,
    )

    assert result.code is GatewayRequestCode.TRANSPORT_ERROR
    assert socket.send.call_count == 2
    assert socket.close.call_count == 2


def test_timeout_reports_timeout_after_final_attempt():
    context, socket = _transport(zmq.Again(), zmq.Again())

    result = _send(
        context,
        {"action": "subscribe_candle"},
        max_attempts=2,
    )

    assert result.code is GatewayRequestCode.TIMEOUT
    assert socket.send.call_count == 2


def test_non_bytes_response_is_classified_as_invalid_response():
    context, socket = _transport(object())

    result = _send(
        context,
        {"action": "subscribe_candle"},
        max_attempts=1,
    )

    assert result.code is GatewayRequestCode.INVALID_RESPONSE
    socket.close.assert_called_once_with()
