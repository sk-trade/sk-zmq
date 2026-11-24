"""Shared test builders and deterministic wait doubles."""

from __future__ import annotations

from unittest.mock import patch

from sk_zmq.client import GatewayRequestCode, GatewayRequestResult, ZMQClient

_COMMON_KWARGS = dict(
    client_id="test-client",
    symbol="KRW-BTC",
    zmq_gateway_host="127.0.0.1",
    zmq_gateway_req_port=11556,
    zmq_gateway_pub_port=11558,
)

STREAM = "a" * 32
RESTART = "b" * 32


def _make_client(
    intervals=None,
    callback=None,
    throttle_seconds=None,
    callback_snapshot_mode="deque_copy",
    candle_deque_maxlen=10,
    exchange="upbit",
    on_critical=None,
    **extra,
):
    """Build a ZMQClient with ZMQ context mocked out so no sockets are created."""
    if intervals is None:
        intervals = ["1m"]
    if callback is None:
        callback = lambda _d: None  # noqa: E731

    with patch("sk_zmq.client.zmq.Context"):
        client = ZMQClient(
            intervals=intervals,
            candle_handler_callback=callback,
            throttle_seconds=throttle_seconds,
            callback_snapshot_mode=callback_snapshot_mode,
            candle_deque_maxlen=candle_deque_maxlen,
            exchange=exchange,
            on_critical=on_critical,
            **_COMMON_KWARGS,
            **extra,
        )
    return client


def _make_candle(ts: int, close: float = 100.0) -> dict:
    return {
        "ts": ts,
        "open": 100.0,
        "high": 105.0,
        "low": 95.0,
        "close": close,
        "volume": 1.0,
    }


def _request_ok(data=None, *, stream_id=STREAM, snapshot_seq=0) -> GatewayRequestResult:
    response = {"status": "ok", "stream_id": stream_id, "snapshot_seq": snapshot_seq}
    if data is not None:
        response["data"] = data
    return GatewayRequestResult.success(data=data, response=response)


def _request_failure(
    code: GatewayRequestCode = GatewayRequestCode.GATEWAY_REJECTED,
) -> GatewayRequestResult:
    response = (
        {"status": "error"} if code is GatewayRequestCode.GATEWAY_REJECTED else None
    )
    return GatewayRequestResult.failure(code, response=response)


def _inject_update(client: ZMQClient, interval: str, candle: dict) -> None:
    """Drive _ingest_publication with an UPDATE payload directly."""
    topic = f"UPBIT:CANDLE:{client.symbol}:{interval}:UPDATE"
    client._ingest_publication(topic, _event_payload(client, candle=candle))


def _inject_close(
    client: ZMQClient, interval: str, closed_candle: dict, new_candle: dict
) -> None:
    topic = f"UPBIT:CANDLE:{client.symbol}:{interval}:CLOSE"
    client._ingest_publication(
        topic, _event_payload(client, candle=closed_candle, new=new_candle)
    )


def _inject_reconcile(client: ZMQClient, interval: str, candle: dict) -> None:
    topic = f"UPBIT:CANDLE:{client.symbol}:{interval}:RECONCILE"
    client._ingest_publication(topic, _event_payload(client, candle=candle))


def _event_payload(client, **fields):
    client._test_seq = getattr(client, "_test_seq", 0) + 1
    return dict(fields, stream_id=STREAM, seq=client._test_seq)


class _FiniteWait:
    def __init__(self, cycles: int):
        self.cycles = cycles
        self.calls = 0

    def __call__(self, timeout=None) -> bool:
        self.calls += 1
        return self.calls > self.cycles


class _StopOnWait:
    def __init__(self):
        self.stopped = False
        self.timeouts = []

    def is_set(self):
        return self.stopped

    def wait(self, timeout=None):
        self.timeouts.append(timeout)
        self.stopped = True
        return True


class _RecordingWait:
    def __init__(self):
        self.timeouts = []

    def __call__(self, timeout=None):
        self.timeouts.append(timeout)
        return True
