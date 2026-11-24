"""Thread-safe candle state and recovery bookkeeping."""

from __future__ import annotations

import threading
from collections import deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TypeAlias

from ._config import SnapshotMode, validate_snapshot_mode
from ._protocol import Candle, CandleEvent, CandleEventType

CandleDeque: TypeAlias = deque[Candle]
CandleDequeMap: TypeAlias = dict[str, CandleDeque]


@dataclass(frozen=True, slots=True)
class RecoveryPlan:
    intervals: tuple[str, ...]
    generation: int


@dataclass(frozen=True, slots=True)
class RecoverySnapshot:
    candles: list[Candle]
    stream_id: str
    sequence: int


@dataclass(frozen=True, slots=True)
class StoreUpdate:
    candles_changed: bool = False
    recovery_requested: bool = False


@dataclass(frozen=True, slots=True)
class RecoveryCommit:
    accepted: bool
    candles_changed: bool = False
    expanded_to_all_intervals: bool = False


@dataclass(frozen=True, slots=True)
class _EventAdmission:
    accepted: bool
    recovery_requested: bool = False


@dataclass(frozen=True, slots=True)
class _RecoveryValidation:
    accepted: bool
    expanded_to_all_intervals: bool = False


class CandleStore:
    """Own candle rows, ordering watermarks, and buffered recovery events."""

    def __init__(self, intervals: Sequence[str], capacity: int) -> None:
        self.intervals = tuple(intervals)
        self.lock = threading.Lock()
        self._candles_by_interval: CandleDequeMap = {
            interval: deque(maxlen=capacity) for interval in self.intervals
        }
        self._buffered_events_by_interval: dict[str, list[CandleEvent]] = {}
        self._sequence_by_interval: dict[str, tuple[str, int]] = {}
        self._target_stream_id: str | None = None
        self._retired_stream_ids: set[str] = set()
        self._recovery_generation = 0

    @property
    def live_candles(self) -> CandleDequeMap:
        return self._candles_by_interval

    def snapshot(self, mode: SnapshotMode = "deep_copy") -> CandleDequeMap:
        snapshot_mode = validate_snapshot_mode(mode)
        with self.lock:
            return self._copy_locked(snapshot_mode)

    def snapshot_if_ready(self, mode: SnapshotMode) -> CandleDequeMap | None:
        with self.lock:
            if self._buffered_events_by_interval:
                return None
            return self._copy_locked(mode)

    def _copy_locked(self, mode: SnapshotMode) -> CandleDequeMap:
        if mode == "live":
            return self._candles_by_interval
        if mode == "deque_copy":
            return {
                interval: deque(candles, maxlen=candles.maxlen)
                for interval, candles in self._candles_by_interval.items()
            }
        return {
            interval: deque(
                (dict(candle) for candle in candles),
                maxlen=candles.maxlen,
            )
            for interval, candles in self._candles_by_interval.items()
        }

    def request_recovery(
        self,
        intervals: Sequence[str] | None = None,
        *,
        stream_id: str | None = None,
    ) -> None:
        with self.lock:
            self._request_recovery_locked(intervals, stream_id=stream_id)

    def _request_recovery_locked(
        self,
        intervals: Sequence[str] | None = None,
        *,
        stream_id: str | None = None,
    ) -> None:
        self._recovery_generation += 1
        if stream_id is not None:
            self._target_stream_id = stream_id
        recovery_intervals = self.intervals if intervals is None else intervals
        for interval in recovery_intervals:
            self._buffered_events_by_interval.setdefault(interval, [])

    def _admit_event_locked(
        self,
        event: CandleEvent,
        *,
        is_buffering: bool,
    ) -> _EventAdmission:
        if event.stream_id in self._retired_stream_ids:
            return _EventAdmission(accepted=False)

        current_ordering = self._sequence_by_interval.get(event.interval)
        recovery_requested = False
        accepted = True

        if current_ordering is None:
            if is_buffering and self._target_stream_id is None:
                self._target_stream_id = event.stream_id
            elif is_buffering and event.stream_id != self._target_stream_id:
                self._request_recovery_locked(stream_id=event.stream_id)
                recovery_requested = True
        else:
            current_stream_id, current_sequence = current_ordering
            if event.stream_id == current_stream_id:
                accepted = event.sequence > current_sequence
            elif event.stream_id != self._target_stream_id:
                self._request_recovery_locked(stream_id=event.stream_id)
                recovery_requested = True

        return _EventAdmission(
            accepted=accepted,
            recovery_requested=recovery_requested,
        )

    def _buffer_event_locked(
        self,
        event: CandleEvent,
        buffered_events: list[CandleEvent],
        *,
        max_buffered_events: int,
        recovery_requested: bool,
    ) -> StoreUpdate:
        if len(buffered_events) >= max_buffered_events:
            buffered_events.clear()
            self._recovery_generation += 1
            recovery_requested = True
        buffered_events.append(event)
        return StoreUpdate(recovery_requested=recovery_requested)

    def ingest(self, event: CandleEvent, *, max_buffered_events: int) -> StoreUpdate:
        with self.lock:
            was_buffering = event.interval in self._buffered_events_by_interval
            admission = self._admit_event_locked(
                event,
                is_buffering=was_buffering,
            )
            if not admission.accepted:
                return StoreUpdate()

            # Runtime admission can create a recovery buffer for this event.
            buffered_events = self._buffered_events_by_interval.get(event.interval)
            if buffered_events is not None:
                return self._buffer_event_locked(
                    event,
                    buffered_events,
                    max_buffered_events=max_buffered_events,
                    recovery_requested=admission.recovery_requested,
                )

            self._sequence_by_interval[event.interval] = (
                event.stream_id,
                event.sequence,
            )
            if event.event_type is CandleEventType.RESYNC_REQUIRED:
                self._request_recovery_locked([event.interval])
                return StoreUpdate(recovery_requested=True)

            return StoreUpdate(candles_changed=self._apply_event_locked(event))

    def begin_recovery(self) -> RecoveryPlan:
        with self.lock:
            return RecoveryPlan(
                intervals=tuple(self._buffered_events_by_interval),
                generation=self._recovery_generation,
            )

    def _validate_recovery_set_locked(
        self,
        plan: RecoveryPlan,
        snapshots: Mapping[str, RecoverySnapshot],
    ) -> _RecoveryValidation:
        if plan.generation != self._recovery_generation:
            return _RecoveryValidation(accepted=False)
        if set(plan.intervals) != set(snapshots):
            return _RecoveryValidation(accepted=False)
        if not plan.intervals:
            return _RecoveryValidation(accepted=True)

        snapshot_stream_ids = {snapshot.stream_id for snapshot in snapshots.values()}
        if len(snapshot_stream_ids) != 1:
            return _RecoveryValidation(accepted=False)
        snapshot_stream_id = next(iter(snapshot_stream_ids))

        is_partial_recovery = set(plan.intervals) != set(self.intervals)
        runtime_conflict = any(
            ordering[0] != snapshot_stream_id
            for ordering in self._sequence_by_interval.values()
        )
        if is_partial_recovery and runtime_conflict:
            self._request_recovery_locked(stream_id=snapshot_stream_id)
            return _RecoveryValidation(
                accepted=False,
                expanded_to_all_intervals=True,
            )
        return _RecoveryValidation(accepted=True)

    def _snapshot_can_commit_locked(
        self,
        interval: str,
        snapshot: RecoverySnapshot,
    ) -> bool:
        current_ordering = self._sequence_by_interval.get(interval)
        if snapshot.stream_id in self._retired_stream_ids:
            return False
        if (
            self._target_stream_id is not None
            and snapshot.stream_id != self._target_stream_id
        ):
            return False
        if (
            current_ordering is not None
            and current_ordering[0] == snapshot.stream_id
            and snapshot.sequence < current_ordering[1]
        ):
            return False

        buffered_events = self._buffered_events_by_interval[interval]
        return not any(
            event.stream_id == snapshot.stream_id
            and event.sequence > snapshot.sequence
            and event.event_type is CandleEventType.RESYNC_REQUIRED
            for event in buffered_events
        )

    def _commit_interval_locked(
        self,
        interval: str,
        snapshot: RecoverySnapshot,
    ) -> bool:
        current_ordering = self._sequence_by_interval.get(interval)
        if current_ordering is not None and current_ordering[0] != snapshot.stream_id:
            self._retired_stream_ids.add(current_ordering[0])

        candles = self._candles_by_interval[interval]
        previous_candles = list(candles)
        candles.clear()
        candles.extend(snapshot.candles)

        committed_sequence = snapshot.sequence
        buffered_events = self._buffered_events_by_interval.pop(interval)
        replayable_events = sorted(
            (
                event
                for event in buffered_events
                if event.stream_id == snapshot.stream_id
                and event.sequence > committed_sequence
            ),
            key=lambda event: event.sequence,
        )
        for event in replayable_events:
            if event.sequence <= committed_sequence:
                continue
            self._apply_event_locked(event)
            committed_sequence = event.sequence

        self._sequence_by_interval[interval] = (
            snapshot.stream_id,
            committed_sequence,
        )
        return list(candles) != previous_candles

    def commit_recovery(
        self,
        plan: RecoveryPlan,
        snapshots: Mapping[str, RecoverySnapshot],
    ) -> RecoveryCommit:
        with self.lock:
            validation = self._validate_recovery_set_locked(plan, snapshots)
            if not validation.accepted:
                return RecoveryCommit(
                    accepted=False,
                    expanded_to_all_intervals=validation.expanded_to_all_intervals,
                )
            if not plan.intervals:
                return RecoveryCommit(accepted=True)
            if not all(
                self._snapshot_can_commit_locked(interval, snapshot)
                for interval, snapshot in snapshots.items()
            ):
                return RecoveryCommit(accepted=False)

            candles_changed = False
            for interval, snapshot in snapshots.items():
                interval_changed = self._commit_interval_locked(interval, snapshot)
                candles_changed = interval_changed or candles_changed
            self._target_stream_id = None
            return RecoveryCommit(
                accepted=True,
                candles_changed=candles_changed,
            )

    def observe_runtime(self, interval: str, stream_id: str) -> bool:
        """Request full recovery when renewal observes a new gateway runtime."""
        with self.lock:
            current_ordering = self._sequence_by_interval.get(interval)
            if (
                current_ordering is None
                or current_ordering[0] == stream_id
                or stream_id in self._retired_stream_ids
                or stream_id == self._target_stream_id
            ):
                return False
            self._request_recovery_locked(stream_id=stream_id)
            return True

    def clear(self) -> None:
        with self.lock:
            self._buffered_events_by_interval.clear()
            self._sequence_by_interval.clear()
            self._target_stream_id = None
            self._retired_stream_ids.clear()
            self._recovery_generation = 0
            for candles in self._candles_by_interval.values():
                candles.clear()

    def _apply_event_locked(self, event: CandleEvent) -> bool:
        candles = self._candles_by_interval[event.interval]
        previous_candles = list(candles)
        candles_by_timestamp = {candle["ts"]: candle for candle in candles}

        if event.event_type is not CandleEventType.RESYNC_REQUIRED:
            candle = event.candle
            if candle is None:
                return False
            if (
                event.event_type is not CandleEventType.RECONCILE
                or candle["ts"] in candles_by_timestamp
            ):
                candles_by_timestamp[candle["ts"]] = candle
        if event.next_candle is not None:
            candles_by_timestamp[event.next_candle["ts"]] = event.next_candle

        candles.clear()
        candles.extend(
            sorted(candles_by_timestamp.values(), key=lambda candle: candle["ts"])
        )
        return list(candles) != previous_candles
