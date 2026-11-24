"""Gateway protocol types and validation helpers.

This module contains the pure, deterministic parts of the candle-stream contract.
Keeping them separate from sockets and threads makes malformed input behavior easy
to test without constructing a client.
"""

from __future__ import annotations

import math
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, TypeAlias

Candle: TypeAlias = dict[str, Any]
_CANDLE_TOPIC_PART_COUNT = 5

SUPPORTED_INTERVALS: tuple[str, ...] = (
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "4h",
)


class GatewayRequestCode(str, Enum):
    OK = "ok"
    CANCELED = "canceled"
    TIMEOUT = "timeout"
    INVALID_REQUEST = "invalid_request"
    TRANSPORT_ERROR = "transport_error"
    INVALID_RESPONSE = "invalid_response"
    GATEWAY_REJECTED = "gateway_rejected"


@dataclass(frozen=True, slots=True)
class GatewayRequestResult:
    code: GatewayRequestCode
    data: Any = None
    response: Mapping[str, Any] | None = None

    @property
    def ok(self) -> bool:
        return self.code is GatewayRequestCode.OK

    @classmethod
    def success(
        cls,
        data: Any = None,
        response: Mapping[str, Any] | None = None,
    ) -> GatewayRequestResult:
        return cls(GatewayRequestCode.OK, data=data, response=response)

    @classmethod
    def failure(
        cls,
        code: GatewayRequestCode,
        response: Mapping[str, Any] | None = None,
    ) -> GatewayRequestResult:
        if code is GatewayRequestCode.OK:
            raise ValueError("failure result cannot use GatewayRequestCode.OK")
        return cls(code, response=response)


class CandleEventType(str, Enum):
    UPDATE = "UPDATE"
    CLOSE = "CLOSE"
    RECONCILE = "RECONCILE"
    RESYNC_REQUIRED = "RESYNC_REQUIRED"


@dataclass(frozen=True, slots=True)
class CandleEvent:
    interval: str
    event_type: CandleEventType
    stream_id: str
    sequence: int
    candle: Candle | None = None
    next_candle: Candle | None = None


@dataclass(frozen=True, slots=True)
class _ParsedCandleTopic:
    interval: str
    event_type: CandleEventType


@dataclass(frozen=True, slots=True)
class _ParsedCandleBodies:
    candle: Candle | None
    next_candle: Candle | None = None


def normalize_intervals(intervals: Sequence[str]) -> list[str]:
    """Validate intervals and return a stable, duplicate-free list."""
    if isinstance(intervals, (str, bytes)):
        raise TypeError("intervals must be a sequence of interval strings")
    if not intervals:
        raise ValueError("intervals must contain at least one supported interval")

    normalized_intervals: list[str] = []
    seen: set[str] = set()
    for interval in intervals:
        if not isinstance(interval, str):
            raise TypeError("each interval must be a string")
        normalized = interval.strip()
        if normalized not in SUPPORTED_INTERVALS:
            supported = ", ".join(SUPPORTED_INTERVALS)
            raise ValueError(
                f"unsupported interval {interval!r}; expected one of: {supported}"
            )
        if normalized not in seen:
            normalized_intervals.append(normalized)
            seen.add(normalized)
    return normalized_intervals


def is_valid_candle(value: object) -> bool:
    if not isinstance(value, dict):
        return False

    timestamp = value.get("ts")
    if not isinstance(timestamp, int) or isinstance(timestamp, bool) or timestamp < 0:
        return False

    volume = value.get("volume", 0)
    return (
        isinstance(volume, (int, float))
        and not isinstance(volume, bool)
        and math.isfinite(volume)
        and volume >= 0
    )


def has_valid_ordering_metadata(
    payload: object,
    *,
    sequence_key: str,
) -> bool:
    if not isinstance(payload, Mapping):
        return False

    stream_id = payload.get("stream_id")
    sequence = payload.get(sequence_key)
    return (
        isinstance(stream_id, str)
        and bool(stream_id)
        and isinstance(sequence, int)
        and not isinstance(sequence, bool)
        and sequence >= 0
    )


def extract_snapshot(result: GatewayRequestResult) -> list[Candle] | None:
    """Return a sorted, timestamp-deduplicated snapshot from a valid result."""
    if not result.ok or not isinstance(result.data, list) or not result.data:
        return None
    if not all(is_valid_candle(candle) for candle in result.data):
        return None

    candles_by_timestamp = {candle["ts"]: candle for candle in result.data}
    return sorted(candles_by_timestamp.values(), key=lambda candle: candle["ts"])


def describe_request_failure(result: GatewayRequestResult) -> str:
    """Return a bounded, printable error detail suitable for logs and callbacks."""
    detail = result.code.value
    if result.code is not GatewayRequestCode.GATEWAY_REJECTED or not result.response:
        return detail

    message = result.response.get("message")
    if not isinstance(message, str):
        return detail

    normalized = " ".join(message.split())
    sanitized = "".join(
        character for character in normalized if character.isprintable()
    )
    sanitized = sanitized[:200]
    return f"{detail}: {sanitized}" if sanitized else detail


def _parse_candle_topic(
    topic: str,
    *,
    expected_exchange_prefix: str,
    expected_symbol: str,
    subscribed_intervals: Collection[str],
) -> _ParsedCandleTopic | None:
    topic_parts = topic.split(":")
    if len(topic_parts) != _CANDLE_TOPIC_PART_COUNT:
        return None

    exchange_prefix, channel, symbol, interval, event_type_value = topic_parts
    if exchange_prefix != expected_exchange_prefix or channel != "CANDLE":
        return None
    if symbol != expected_symbol or interval not in subscribed_intervals:
        return None

    try:
        event_type = CandleEventType(event_type_value)
    except ValueError:
        return None
    return _ParsedCandleTopic(interval=interval, event_type=event_type)


def _parse_candle_bodies(
    event_type: CandleEventType,
    payload: Mapping[str, Any],
) -> _ParsedCandleBodies | None:
    if event_type is CandleEventType.RESYNC_REQUIRED:
        return _ParsedCandleBodies(candle=None)

    candle = payload.get("candle")
    if not is_valid_candle(candle) or not isinstance(candle, dict):
        return None
    if event_type is not CandleEventType.CLOSE:
        return _ParsedCandleBodies(candle=candle)

    next_candle = payload.get("new")
    if not is_valid_candle(next_candle) or not isinstance(next_candle, dict):
        return None
    if next_candle["ts"] <= candle["ts"]:
        return None
    return _ParsedCandleBodies(candle=candle, next_candle=next_candle)


def parse_candle_event(
    topic: str,
    payload: object,
    *,
    expected_exchange_prefix: str,
    expected_symbol: str,
    subscribed_intervals: Collection[str],
) -> CandleEvent | None:
    """Parse a gateway publication that belongs to this client."""
    parsed_topic = _parse_candle_topic(
        topic,
        expected_exchange_prefix=expected_exchange_prefix,
        expected_symbol=expected_symbol,
        subscribed_intervals=subscribed_intervals,
    )
    if parsed_topic is None or not isinstance(payload, dict):
        return None
    if not has_valid_ordering_metadata(payload, sequence_key="seq"):
        return None

    parsed_bodies = _parse_candle_bodies(parsed_topic.event_type, payload)
    if parsed_bodies is None:
        return None

    return CandleEvent(
        interval=parsed_topic.interval,
        event_type=parsed_topic.event_type,
        stream_id=payload["stream_id"],
        sequence=payload["seq"],
        candle=parsed_bodies.candle,
        next_candle=parsed_bodies.next_candle,
    )
