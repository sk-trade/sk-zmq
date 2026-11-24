"""Constructor validation and public client surface."""

from __future__ import annotations

import threading
from collections import deque
from unittest.mock import patch

import pytest

from sk_zmq import ClientState, ZMQClient

from ._helpers import _inject_update, _make_candle, _make_client


@pytest.mark.parametrize(
    ("field", "value", "error_type"),
    [
        ("client_id", "   ", ValueError),
        ("symbol", "", ValueError),
        ("exchange", "\t", ValueError),
        ("zmq_gateway_host", "", ValueError),
        ("zmq_gateway_req_port", 0, ValueError),
        ("zmq_gateway_req_port", 65_536, ValueError),
        ("zmq_gateway_pub_port", True, TypeError),
        ("server_candle_ttl", 0, ValueError),
        ("server_candle_ttl", float("inf"), ValueError),
        ("server_candle_ttl", True, TypeError),
        ("candle_deque_maxlen", 0, ValueError),
        ("candle_deque_maxlen", True, TypeError),
        ("throttle_seconds", -0.1, ValueError),
        ("throttle_seconds", float("nan"), ValueError),
        ("throttle_seconds", True, TypeError),
        ("callback_snapshot_mode", "copy", ValueError),
    ],
)
def test_invalid_configuration_is_rejected_before_context_allocation(
    field,
    value,
    error_type,
):
    kwargs = {
        "client_id": "client",
        "symbol": "KRW-BTC",
        "intervals": ["1m"],
        "candle_handler_callback": lambda _candles: None,
        field: value,
    }

    with patch("sk_zmq.client.zmq.Context") as context, pytest.raises(error_type):
        ZMQClient(**kwargs)

    context.assert_not_called()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("candle_handler_callback", None),
        ("on_critical", "not-callable"),
    ],
)
def test_callbacks_must_be_callable(field, value):
    kwargs = {
        "client_id": "client",
        "symbol": "KRW-BTC",
        "intervals": ["1m"],
        "candle_handler_callback": lambda _candles: None,
        field: value,
    }

    with patch("sk_zmq.client.zmq.Context") as context, pytest.raises(TypeError):
        ZMQClient(**kwargs)

    context.assert_not_called()


@pytest.mark.parametrize(
    "intervals",
    [
        [],
        "1m",
        ["1m", 5],
        ["1m", "1d"],
    ],
)
def test_intervals_require_a_supported_string_sequence(intervals):
    with (
        patch("sk_zmq.client.zmq.Context") as context,
        pytest.raises((TypeError, ValueError)),
    ):
        ZMQClient(
            client_id="client",
            symbol="KRW-BTC",
            intervals=intervals,
            candle_handler_callback=lambda _candles: None,
        )

    context.assert_not_called()


def test_configuration_is_normalized_without_changing_public_attribute_names():
    client = _make_client(
        intervals=[" 1m ", "1m", "5m"],
        exchange=" UpBit ",
        throttle_seconds=0,
    )

    assert client.client_id == "test-client"
    assert client.intervals == ["1m", "5m"]
    assert client.exchange == "upbit"
    assert client.exchange_prefix == "UPBIT"
    assert client.throttle_seconds == 0.0
    assert client.state is ClientState.CREATED
    assert not client.is_running


def test_public_snapshot_defaults_to_an_independent_deep_copy():
    client = _make_client()
    _inject_update(client, "1m", _make_candle(1))

    snapshot = client.get_candle_snapshot()
    assert isinstance(snapshot["1m"], deque)
    snapshot["1m"][0]["close"] = 999
    snapshot["1m"].clear()

    assert client.candle_deques["1m"][0]["close"] == 100


def test_context_manager_starts_and_stops_the_client():
    client = _make_client()
    with (
        patch.object(client, "start", return_value=True) as start,
        patch.object(client, "stop") as stop,
        client as entered,
    ):
        assert entered is client

    start.assert_called_once_with()
    stop.assert_called_once_with()


def test_context_manager_releases_resources_when_startup_fails():
    client = _make_client()
    with (
        patch.object(client, "start", return_value=False),
        patch.object(client, "stop") as stop,
        pytest.raises(RuntimeError, match="could not start"),
        client,
    ):
        pass

    stop.assert_called_once_with()


def test_running_state_does_not_hide_a_dead_worker():
    client = _make_client()
    client._state = ClientState.RUNNING
    client._worker_threads = [threading.Thread()]
    assert not client.is_running
