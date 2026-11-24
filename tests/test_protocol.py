"""Pure candle gateway protocol validation."""

from __future__ import annotations

import pytest

from sk_zmq._protocol import (
    CandleEventType,
    GatewayRequestCode,
    GatewayRequestResult,
    describe_request_failure,
    extract_snapshot,
    has_valid_ordering_metadata,
    is_valid_candle,
    parse_candle_event,
)

from ._helpers import STREAM, _make_candle


@pytest.mark.parametrize(
    "candle",
    [
        None,
        {},
        {"ts": True},
        {"ts": -1},
        {"ts": 1, "volume": True},
        {"ts": 1, "volume": -1},
        {"ts": 1, "volume": float("inf")},
        {"ts": 1, "volume": float("nan")},
    ],
)
def test_invalid_candle_shapes_are_rejected(candle):
    assert not is_valid_candle(candle)


def test_snapshot_is_sorted_and_uses_the_last_row_for_duplicate_timestamps():
    first = _make_candle(2, 102)
    replacement = _make_candle(2, 202)
    result = GatewayRequestResult.success(
        [_make_candle(3), first, _make_candle(1), replacement]
    )

    snapshot = extract_snapshot(result)

    assert [candle["ts"] for candle in snapshot] == [1, 2, 3]
    assert snapshot[1]["close"] == 202


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        {"stream_id": "", "seq": 1},
        {"stream_id": STREAM, "seq": True},
        {"stream_id": STREAM, "seq": -1},
    ],
)
def test_ordering_metadata_requires_a_stream_id_and_non_negative_integer(payload):
    assert not has_valid_ordering_metadata(payload, sequence_key="seq")


def test_resync_event_does_not_require_a_candle_body():
    event = parse_candle_event(
        "UPBIT:CANDLE:KRW-BTC:1m:RESYNC_REQUIRED",
        {"stream_id": STREAM, "seq": 7, "reason": "gap"},
        expected_exchange_prefix="UPBIT",
        expected_symbol="KRW-BTC",
        subscribed_intervals={"1m"},
    )

    assert event is not None
    assert event.event_type is CandleEventType.RESYNC_REQUIRED
    assert event.sequence == 7
    assert event.candle is None


def test_gateway_error_detail_is_printable_single_line_and_bounded():
    result = GatewayRequestResult.failure(
        GatewayRequestCode.GATEWAY_REJECTED,
        response={"message": "  bad\n request\x00 " + "x" * 300},
    )

    detail = describe_request_failure(result)

    assert detail.startswith("gateway_rejected: bad request")
    assert "\n" not in detail
    assert "\x00" not in detail
    assert len(detail.removeprefix("gateway_rejected: ")) <= 200
