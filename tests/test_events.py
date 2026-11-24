"""Candle validation, ordering, buffering, and recovery behavior."""

from __future__ import annotations

import threading
import time
from unittest.mock import patch

import pytest

from sk_zmq.client import GatewayRequestResult

from ._helpers import (
    RESTART,
    STREAM,
    _event_payload,
    _FiniteWait,
    _inject_close,
    _inject_reconcile,
    _inject_update,
    _make_candle,
    _make_client,
    _request_failure,
    _request_ok,
)


@pytest.mark.parametrize("interval", ["10m", "1d", "1s", "2m"])
def test_reject_unsupported_aggregated_intervals(interval):
    with pytest.raises(ValueError):
        _make_client(intervals=[interval])


@pytest.mark.parametrize("mode", ["live", "deque_copy", "deep_copy"])
def test_callback_mutation_semantics(mode):
    client = _make_client(callback_snapshot_mode=mode)
    _inject_update(client, "1m", _make_candle(1))
    payload = client._build_callback_snapshot()
    payload["1m"][0]["close"] = 123
    assert client.candle_deques["1m"][0]["close"] == (
        100 if mode == "deep_copy" else 123
    )
    payload["1m"].clear()
    assert len(client.candle_deques["1m"]) == (0 if mode == "live" else 1)


def test_event_integrity_and_late_messages():
    client = _make_client(intervals=["1m", "15m"])
    _inject_update(client, "1m", _make_candle(1))
    _inject_close(client, "1m", _make_candle(1, 101), _make_candle(2))
    _inject_update(client, "1m", _make_candle(3))
    client._data_changed.clear()
    client._ingest_publication(
        "UPBIT:CANDLE:KRW-BTC:1m:UPDATE",
        dict(candle=_make_candle(1, 90), stream_id=STREAM, seq=1),
    )
    client._ingest_publication(
        "UPBIT:CANDLE:KRW-BTC:1m:CLOSE",
        dict(
            candle=_make_candle(1, 90), new=_make_candle(2, 90), stream_id=STREAM, seq=2
        ),
    )
    _inject_reconcile(client, "1m", _make_candle(999))
    assert not client._data_changed.is_set()
    assert [c["ts"] for c in client.candle_deques["1m"]] == [1, 2, 3]
    assert client.candle_deques["1m"][0]["close"] == 101
    _inject_reconcile(client, "1m", _make_candle(1, 102))
    assert client.candle_deques["1m"][0]["close"] == 102
    assert client._data_changed.is_set()
    assert not client.candle_deques["15m"]


@pytest.mark.parametrize(
    ("topic", "payload"),
    [
        ("BINANCE:CANDLE:KRW-BTC:1m:UPDATE", {"candle": {"ts": 1}}),
        ("UPBIT:CANDLE:KRW-ETH:1m:UPDATE", {"candle": {"ts": 1}}),
        ("UPBIT:TRADE:KRW-BTC:1m:UPDATE", {"candle": {"ts": 1}}),
        ("UPBIT:CANDLE:KRW-BTC:5m:UPDATE", {"candle": {"ts": 1}}),
        ("UPBIT:CANDLE:KRW-BTC:1m:UPDATE", {"candle": {"ts": "bad"}}),
        ("UPBIT:CANDLE:KRW-BTC:1m:UPDATE", {"candle": {}}),
        ("UPBIT:CANDLE:KRW-BTC:1m:CLOSE", {"candle": {"ts": 1}}),
        ("UPBIT:CANDLE:KRW-BTC:1m:CLOSE", {"candle": {"ts": 2}, "new": {"ts": 1}}),
        ("UPBIT:CANDLE:KRW-BTC:1m:UNKNOWN", {"candle": {"ts": 1}}),
    ],
)
def test_ignore_unrelated_or_malformed_event(topic, payload):
    client = _make_client()
    client._ingest_publication(topic, dict(payload, stream_id=STREAM, seq=1))
    assert not client.candle_deques["1m"]
    assert not client._data_changed.is_set()


def test_snapshot_merges_live_close_without_duplicate_or_regression():
    client = _make_client()
    client._mark_recovery_required()

    def snapshot(request):
        _inject_update(client, "1m", _make_candle(2, 102))
        _inject_close(client, "1m", _make_candle(2, 103), _make_candle(3, 103))
        client._ingest_publication(
            "UPBIT:CANDLE:KRW-BTC:1m:UPDATE",
            dict(candle=_make_candle(2, 99), stream_id=STREAM, seq=1),
        )
        return _request_ok([_make_candle(1), _make_candle(2)])

    with patch.object(client, "_request_gateway", side_effect=snapshot):
        assert client._recover_pending_intervals()
    assert [c["ts"] for c in client.candle_deques["1m"]] == [1, 2, 3]
    assert [c["close"] for c in client.candle_deques["1m"]] == [100, 103, 103]


def test_failed_recovery_preserves_storage_and_retries_buffer():
    client = _make_client()
    _inject_update(client, "1m", _make_candle(1))
    client._ingest_publication(
        "UPBIT:CANDLE:KRW-BTC:1m:RESYNC_REQUIRED", _event_payload(client, reason="gap")
    )
    _inject_update(client, "1m", _make_candle(3, 103))
    with patch.object(client, "_request_gateway", return_value=_request_failure()):
        assert not client._recover_pending_intervals()
    assert [c["ts"] for c in client.candle_deques["1m"]] == [1]
    with patch.object(
        client,
        "_request_gateway",
        return_value=_request_ok([_make_candle(2)], snapshot_seq=2),
    ):
        assert client._recover_pending_intervals()
    assert [c["ts"] for c in client.candle_deques["1m"]] == [2, 3]


def test_buffer_overflow_or_new_resync_invalidates_inflight_snapshot():
    client = _make_client()
    client._MAX_BUFFERED_EVENTS_PER_INTERVAL = 1
    client._mark_recovery_required()

    def snapshot(request):
        _inject_update(client, "1m", _make_candle(2))
        _inject_update(client, "1m", _make_candle(3))
        return _request_ok([_make_candle(1)])

    with patch.object(client, "_request_gateway", side_effect=snapshot):
        assert not client._recover_pending_intervals()
    assert not client.candle_deques["1m"]
    with patch.object(
        client, "_request_gateway", return_value=_request_ok([_make_candle(2)])
    ):
        assert client._recover_pending_intervals()
    assert [c["ts"] for c in client.candle_deques["1m"]] == [2, 3]


def test_callback_loop_releases_lock_and_survives_callback_error():
    calls = []
    done = threading.Event()

    def callback(payload):
        assert client._store.lock.acquire(blocking=False)
        client._store.lock.release()
        calls.append(payload["1m"][-1]["ts"])
        if len(calls) == 1:
            raise RuntimeError("user callback error")
        done.set()

    client = _make_client(callback=callback, throttle_seconds=0.01)
    worker = threading.Thread(target=client._run_callback_loop)
    worker.start()
    try:
        _inject_update(client, "1m", _make_candle(1))
        deadline = time.monotonic() + 2
        while not calls and time.monotonic() < deadline:
            time.sleep(0.01)
        _inject_update(client, "1m", _make_candle(2))
        assert done.wait(2)
        assert calls == [1, 2]
    finally:
        client._stop_requested.set()
        worker.join(2)


def test_sequence_merges_equal_volume_updates_and_discards_snapshot_covered_events():
    client = _make_client()
    client._mark_recovery_required()
    topic = "UPBIT:CANDLE:KRW-BTC:1m:UPDATE"
    for stream_id, seq, close in [
        (STREAM, 12, 112),
        (STREAM, 9, 99),
        (RESTART, 100, 999),
        (STREAM, 11, 111),
        (STREAM, 12, 90),
    ]:
        client._ingest_publication(
            topic, dict(candle=_make_candle(2, close), stream_id=stream_id, seq=seq)
        )
    snapshot = _make_candle(2, 110)
    with patch.object(
        client,
        "_request_gateway",
        return_value=_request_ok([snapshot], snapshot_seq=10),
    ):
        assert client._recover_pending_intervals()
    assert client.candle_deques["1m"][-1]["close"] == 112
    client._data_changed.clear()
    # Covered and duplicate events cannot mutate storage or trigger a callback.
    for seq in (10, 11, 12):
        client._ingest_publication(
            topic, dict(candle=_make_candle(2, 90), stream_id=STREAM, seq=seq)
        )
    assert not client._data_changed.is_set()
    # A later sequence is authoritative even when volume decreases.
    latest = _make_candle(2, 113)
    latest["volume"] = 0
    client._ingest_publication(topic, dict(candle=latest, stream_id=STREAM, seq=13))
    assert client.candle_deques["1m"][-1] == latest


def test_stale_snapshot_does_not_regress_existing_sequence():
    client = _make_client()
    _inject_update(client, "1m", _make_candle(3))
    client._mark_recovery_required()
    with patch.object(
        client,
        "_request_gateway",
        return_value=_request_ok([_make_candle(2)], snapshot_seq=0),
    ):
        assert not client._recover_pending_intervals()
    assert [c["ts"] for c in client.candle_deques["1m"]] == [3]


def test_restart_resnapshots_all_intervals_without_comparing_runtime_counters():
    client = _make_client(intervals=["1m", "15m"])
    client._mark_recovery_required()
    with patch.object(
        client,
        "_request_gateway",
        return_value=_request_ok([_make_candle(1)], snapshot_seq=100),
    ):
        assert client._recover_pending_intervals()
    # New runtime seq=1 must not be discarded against old runtime seq=100.
    client._ingest_publication(
        "UPBIT:CANDLE:KRW-BTC:1m:UPDATE",
        dict(candle=_make_candle(2, 102), stream_id=RESTART, seq=1),
    )
    with patch.object(
        client,
        "_request_gateway",
        return_value=_request_ok([_make_candle(1)], snapshot_seq=101),
    ):
        assert not client._recover_pending_intervals()
    assert all(c[-1]["ts"] == 1 for c in client.candle_deques.values())
    with patch.object(
        client,
        "_request_gateway",
        return_value=_request_ok(
            [_make_candle(2, 100)], stream_id=RESTART, snapshot_seq=0
        ),
    ) as request:
        assert client._recover_pending_intervals()
    assert {call.args[0]["interval"] for call in request.call_args_list} == {
        "1m",
        "15m",
    }
    assert client.candle_deques["1m"][-1]["close"] == 102
    assert client.candle_deques["15m"][-1]["close"] == 100
    client._data_changed.clear()
    client._ingest_publication(
        "UPBIT:CANDLE:KRW-BTC:1m:UPDATE",
        dict(candle=_make_candle(3, 999), stream_id=STREAM, seq=1000),
    )
    assert not client._data_changed.is_set()
    assert client.candle_deques["1m"][-1]["ts"] == 2
    client._ingest_publication(
        "UPBIT:CANDLE:KRW-BTC:1m:UPDATE",
        dict(candle=_make_candle(2, 103), stream_id=RESTART, seq=2),
    )
    assert client.candle_deques["1m"][-1]["close"] == 103


def test_control_after_snapshot_boundary_requires_new_snapshot():
    client = _make_client()
    client._mark_recovery_required()
    client._ingest_publication(
        "UPBIT:CANDLE:KRW-BTC:1m:RESYNC_REQUIRED", dict(stream_id=STREAM, seq=11)
    )
    with patch.object(
        client,
        "_request_gateway",
        return_value=_request_ok([_make_candle(1)], snapshot_seq=10),
    ):
        assert not client._recover_pending_intervals()
    assert not client.candle_deques["1m"]
    with patch.object(
        client,
        "_request_gateway",
        return_value=_request_ok([_make_candle(2)], snapshot_seq=11),
    ):
        assert client._recover_pending_intervals()
    client._data_changed.clear()
    # A duplicate control notice must not restart recovery.
    client._ingest_publication(
        "UPBIT:CANDLE:KRW-BTC:1m:RESYNC_REQUIRED", dict(stream_id=STREAM, seq=11)
    )
    client._ingest_publication(
        "UPBIT:CANDLE:KRW-BTC:1m:UPDATE",
        dict(candle=_make_candle(2, 102), stream_id=STREAM, seq=12),
    )
    assert client.candle_deques["1m"][-1]["close"] == 102


def test_restart_detected_by_renewal_keeps_data_until_full_snapshot():
    client = _make_client(intervals=["1m", "15m"])
    client._mark_recovery_required()
    with patch.object(
        client,
        "_request_gateway",
        return_value=_request_ok([_make_candle(1)], snapshot_seq=100),
    ):
        assert client._recover_pending_intervals()
    with (
        patch.object(
            client,
            "_request_gateway",
            return_value=_request_ok(
                [_make_candle(2)], stream_id=RESTART, snapshot_seq=1
            ),
        ),
        patch.object(type(client._stop_requested), "wait", _FiniteWait(1)),
    ):
        client._run_renewal_loop()
    assert all(c[-1]["ts"] == 1 for c in client.candle_deques.values())
    with patch.object(
        client,
        "_request_gateway",
        return_value=_request_ok([_make_candle(2)], stream_id=RESTART, snapshot_seq=1),
    ) as request:
        assert client._recover_pending_intervals()
    assert request.call_count == 2
    assert all(c[-1]["ts"] == 2 for c in client.candle_deques.values())


def test_snapshot_restart_during_one_interval_recovery_expands_to_all():
    client = _make_client(intervals=["1m", "15m"])
    client._mark_recovery_required()
    with patch.object(
        client,
        "_request_gateway",
        return_value=_request_ok([_make_candle(1)], snapshot_seq=100),
    ):
        assert client._recover_pending_intervals()
    client._ingest_publication(
        "UPBIT:CANDLE:KRW-BTC:1m:RESYNC_REQUIRED", dict(stream_id=STREAM, seq=101)
    )
    with patch.object(
        client,
        "_request_gateway",
        return_value=_request_ok([_make_candle(2)], stream_id=RESTART, snapshot_seq=0),
    ) as request:
        assert not client._recover_pending_intervals()
        assert client._recover_pending_intervals()
    assert request.call_count == 3  # one interval, then both after detecting restart
    assert all(c[-1]["ts"] == 2 for c in client.candle_deques.values())


@pytest.mark.parametrize(
    "metadata",
    [
        {},
        {"stream_id": STREAM},
        {"stream_id": "", "snapshot_seq": 1},
        {"stream_id": STREAM, "snapshot_seq": -1},
        {"stream_id": STREAM, "snapshot_seq": True},
    ],
)
def test_missing_or_invalid_snapshot_boundary_cannot_publish_history(metadata):
    client = _make_client()
    client._mark_recovery_required()
    result = GatewayRequestResult.success(
        data=[_make_candle(1)], response=dict(status="ok", **metadata)
    )
    with patch.object(client, "_request_gateway", return_value=result):
        assert not client._recover_pending_intervals()
    assert not client.candle_deques["1m"]


def test_live_sequence_upserts_timestamp_without_replacing_newer_bucket():
    client = _make_client()
    _inject_close(client, "1m", _make_candle(1), _make_candle(2))
    _inject_update(client, "1m", _make_candle(1, 101))
    assert [c["ts"] for c in client.candle_deques["1m"]] == [1, 2]
    assert client.candle_deques["1m"][0]["close"] == 101


def test_restart_during_snapshot_request_invalidates_old_runtime_response():
    client = _make_client()
    client._mark_recovery_required()
    with patch.object(
        client,
        "_request_gateway",
        return_value=_request_ok([_make_candle(1)], snapshot_seq=100),
    ):
        assert client._recover_pending_intervals()
    client._mark_recovery_required()

    def restarting_snapshot(request):
        client._ingest_publication(
            "UPBIT:CANDLE:KRW-BTC:1m:UPDATE",
            dict(candle=_make_candle(2, 102), stream_id=RESTART, seq=1),
        )
        return _request_ok([_make_candle(3)], snapshot_seq=101)

    with patch.object(client, "_request_gateway", side_effect=restarting_snapshot):
        assert not client._recover_pending_intervals()
    assert client.candle_deques["1m"][-1]["ts"] == 1
    with patch.object(
        client,
        "_request_gateway",
        return_value=_request_ok([_make_candle(2)], stream_id=RESTART, snapshot_seq=0),
    ):
        assert client._recover_pending_intervals()
    assert client.candle_deques["1m"][-1]["close"] == 102


@pytest.mark.parametrize(
    "metadata",
    [
        {},
        {"stream_id": STREAM, "seq": True},
        {"stream_id": STREAM, "seq": -1},
        {"stream_id": "", "seq": 1},
    ],
)
def test_unordered_events_cannot_mutate_candles_or_trigger_recovery(metadata):
    client = _make_client()
    client._ingest_publication(
        "UPBIT:CANDLE:KRW-BTC:1m:UPDATE", dict(candle=_make_candle(1), **metadata)
    )
    assert not client.candle_deques["1m"]
    assert not client._data_changed.is_set()


def test_first_buffered_runtime_invalidates_a_snapshot_from_another_runtime():
    client = _make_client()
    client._mark_recovery_required()

    def stale_snapshot(_request):
        client._ingest_publication(
            "UPBIT:CANDLE:KRW-BTC:1m:UPDATE",
            dict(candle=_make_candle(2, 102), stream_id=RESTART, seq=1),
        )
        return _request_ok([_make_candle(1)], stream_id=STREAM, snapshot_seq=10)

    with patch.object(client, "_request_gateway", side_effect=stale_snapshot):
        assert not client._recover_pending_intervals()

    assert not client.candle_deques["1m"]
    with patch.object(
        client,
        "_request_gateway",
        return_value=_request_ok(
            [_make_candle(2, 100)],
            stream_id=RESTART,
            snapshot_seq=0,
        ),
    ):
        assert client._recover_pending_intervals()

    assert client.candle_deques["1m"][-1]["close"] == 102


def test_new_recovery_requested_during_commit_remains_signaled():
    client = _make_client()
    client._mark_recovery_required()
    commit_recovery = client._store.commit_recovery

    def commit_and_request_recovery(plan, snapshots):
        commit = commit_recovery(plan, snapshots)
        client._mark_recovery_required()
        return commit

    with (
        patch.object(
            client, "_request_gateway", return_value=_request_ok([_make_candle(1)])
        ),
        patch.object(
            client._store, "commit_recovery", side_effect=commit_and_request_recovery
        ),
    ):
        assert client._recover_pending_intervals()
    assert client._recovery_requested.is_set()

    with patch.object(
        client, "_request_gateway", return_value=_request_ok([_make_candle(1)])
    ):
        assert client._recover_pending_intervals()
    assert not client._recovery_requested.is_set()


def test_callback_deferred_by_recovery_survives_unchanged_snapshot():
    delivered = threading.Event()
    blocked = threading.Event()
    client = _make_client(callback=lambda _snapshot: delivered.set())
    _inject_update(client, "1m", _make_candle(1))
    client._mark_recovery_required()
    snapshot_if_ready = client._store.snapshot_if_ready

    def observe_snapshot(mode):
        snapshot = snapshot_if_ready(mode)
        if snapshot is None:
            blocked.set()
        return snapshot

    worker = threading.Thread(target=client._run_callback_loop)
    with patch.object(client._store, "snapshot_if_ready", side_effect=observe_snapshot):
        worker.start()
        try:
            assert blocked.wait(2)
            assert not delivered.is_set()
            with patch.object(
                client,
                "_request_gateway",
                return_value=_request_ok([_make_candle(1)], snapshot_seq=1),
            ):
                assert client._recover_pending_intervals()
            assert delivered.wait(2)
        finally:
            client._stop_requested.set()
            client._data_changed.set()
            worker.join(2)
    assert not worker.is_alive()
