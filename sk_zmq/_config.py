"""Validated client configuration."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from numbers import Real
from typing import Literal, TypeAlias

from ._protocol import normalize_intervals

SnapshotMode: TypeAlias = Literal["live", "deque_copy", "deep_copy"]
SNAPSHOT_MODES: tuple[SnapshotMode, ...] = ("live", "deque_copy", "deep_copy")
_MIN_TCP_PORT = 1
_MAX_TCP_PORT = 65_535


@dataclass(frozen=True, slots=True)
class ClientConfig:
    client_id: str
    symbol: str
    intervals: tuple[str, ...]
    exchange: str
    gateway_host: str
    gateway_request_port: int
    gateway_publish_port: int
    subscription_ttl_seconds: float
    candle_capacity: int
    callback_throttle_seconds: float | None
    callback_snapshot_mode: SnapshotMode

    @property
    def exchange_prefix(self) -> str:
        return self.exchange.upper()

    @classmethod
    def create(
        cls,
        *,
        client_id: str,
        symbol: str,
        intervals: Sequence[str],
        exchange: str,
        gateway_host: str,
        gateway_request_port: int,
        gateway_publish_port: int,
        subscription_ttl_seconds: float,
        candle_capacity: int,
        callback_throttle_seconds: float | None,
        callback_snapshot_mode: str,
    ) -> ClientConfig:
        return cls(
            client_id=_non_empty_text(client_id, field_name="client_id"),
            symbol=_non_empty_text(symbol, field_name="symbol"),
            intervals=tuple(normalize_intervals(intervals)),
            exchange=_non_empty_text(exchange, field_name="exchange").lower(),
            gateway_host=_non_empty_text(
                gateway_host,
                field_name="zmq_gateway_host",
            ),
            gateway_request_port=_port(
                gateway_request_port,
                field_name="zmq_gateway_req_port",
            ),
            gateway_publish_port=_port(
                gateway_publish_port,
                field_name="zmq_gateway_pub_port",
            ),
            subscription_ttl_seconds=_positive_seconds(
                subscription_ttl_seconds,
                field_name="server_candle_ttl",
            ),
            candle_capacity=_positive_integer(
                candle_capacity,
                field_name="candle_deque_maxlen",
            ),
            callback_throttle_seconds=_optional_non_negative_seconds(
                callback_throttle_seconds,
                field_name="throttle_seconds",
            ),
            callback_snapshot_mode=validate_snapshot_mode(callback_snapshot_mode),
        )


def _non_empty_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be a string")
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


def _port(value: object, *, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field_name} must be an integer")
    if not _MIN_TCP_PORT <= value <= _MAX_TCP_PORT:
        raise ValueError(
            f"{field_name} must be between {_MIN_TCP_PORT} and {_MAX_TCP_PORT}"
        )
    return value


def _positive_seconds(value: object, *, field_name: str) -> float:
    if not isinstance(value, Real) or isinstance(value, bool):
        raise TypeError(f"{field_name} must be a number")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized <= 0:
        raise ValueError(f"{field_name} must be a finite number greater than 0")
    return normalized


def _optional_non_negative_seconds(
    value: object,
    *,
    field_name: str,
) -> float | None:
    if value is None:
        return None
    if not isinstance(value, Real) or isinstance(value, bool):
        raise TypeError(f"{field_name} must be a number or None")
    normalized = float(value)
    if not math.isfinite(normalized) or normalized < 0:
        raise ValueError(f"{field_name} must be finite and greater than or equal to 0")
    return normalized


def _positive_integer(value: object, *, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field_name} must be an integer")
    if value <= 0:
        raise ValueError(f"{field_name} must be greater than 0")
    return value


def validate_snapshot_mode(value: object) -> SnapshotMode:
    if not isinstance(value, str):
        raise TypeError("callback_snapshot_mode must be a string")
    if value not in SNAPSHOT_MODES:
        expected = ", ".join(SNAPSHOT_MODES)
        raise ValueError(f"callback_snapshot_mode must be one of: {expected}")
    return value
