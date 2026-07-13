"""
Focused unit tests for ZMQClient.

No real ZMQ gateway is required.  All ZMQ socket I/O is prevented by
patching `ZMQClient._send_request` and `zmq.Context` so the client
under test never dials out.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from unittest.mock import MagicMock, patch

import pytest
import zmq

from sk_zmq.client import ZMQClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_COMMON_KWARGS = dict(
    client_id="test-client",
    symbol="KRW-BTC",
    zmq_gateway_host="127.0.0.1",
    zmq_gateway_req_port=11556,
    zmq_gateway_pub_port=11558,
)


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
    return {"ts": ts, "open": 100.0, "high": 105.0, "low": 95.0, "close": close, "volume": 1.0}


def _inject_update(client: ZMQClient, interval: str, candle: dict) -> None:
    """Drive _handle_candle_event with an UPDATE payload directly."""
    topic = f"UPBIT:CANDLE:{client.symbol}:{interval}:UPDATE"
    client._handle_candle_event(topic, {"candle": candle})


def _inject_close(client: ZMQClient, interval: str, closed_candle: dict, new_candle: dict) -> None:
    topic = f"UPBIT:CANDLE:{client.symbol}:{interval}:CLOSE"
    client._handle_candle_event(topic, {"candle": closed_candle, "new": new_candle})


def _inject_reconcile(client: ZMQClient, interval: str, candle: dict) -> None:
    topic = f"UPBIT:CANDLE:{client.symbol}:{interval}:RECONCILE"
    client._handle_candle_event(topic, {"candle": candle})


# ===========================================================================
# 1. Interval Validation
# ===========================================================================

class TestIntervalValidation:
    def test_valid_intervals_accepted(self):
        valid = ["1m", "3m", "5m", "10m", "30m", "1h", "4h", "1d"]
        for iv in valid:
            c = _make_client(intervals=[iv])
            assert iv in c.candle_deques

    def test_whitespace_stripped_around_valid_interval(self):
        c = _make_client(intervals=[" 1m "])
        assert "1m" in c.candle_deques

    def test_invalid_interval_raises(self):
        with pytest.raises(ValueError, match="Invalid interval"):
            _make_client(intervals=["2m"])

    def test_mixed_invalid_raises(self):
        with pytest.raises(ValueError, match="Invalid interval"):
            _make_client(intervals=["1m", "bad"])

    def test_multiple_valid_intervals_all_present(self):
        c = _make_client(intervals=["1m", "5m", "1h"])
        assert set(c.candle_deques.keys()) == {"1m", "5m", "1h"}

    def test_deque_maxlen_applied(self):
        c = _make_client(intervals=["1m"], candle_deque_maxlen=42)
        assert c.candle_deques["1m"].maxlen == 42


# ===========================================================================
# 2. Callback Snapshot Mode Validation
# ===========================================================================

class TestCallbackSnapshotModeValidation:
    def test_live_accepted(self):
        c = _make_client(callback_snapshot_mode="live")
        assert c.callback_snapshot_mode == "live"

    def test_deque_copy_accepted(self):
        c = _make_client(callback_snapshot_mode="deque_copy")
        assert c.callback_snapshot_mode == "deque_copy"

    def test_deep_copy_accepted(self):
        c = _make_client(callback_snapshot_mode="deep_copy")
        assert c.callback_snapshot_mode == "deep_copy"

    def test_invalid_mode_raises(self):
        with pytest.raises(ValueError, match="Invalid callback_snapshot_mode"):
            _make_client(callback_snapshot_mode="snapshot")


# ===========================================================================
# 3. _build_callback_payload snapshot modes
# ===========================================================================

class TestBuildCallbackPayload:
    """Tests that the three snapshot modes return payloads with the correct
    identity / copy semantics, and that the call releases the storage lock
    (i.e., does not hold the lock after returning)."""

    def _loaded_client(self, mode: str) -> ZMQClient:
        c = _make_client(intervals=["1m", "5m"], callback_snapshot_mode=mode)
        for iv in ["1m", "5m"]:
            c.candle_deques[iv].extend([_make_candle(i) for i in range(3)])
        return c

    # -- live mode -----------------------------------------------------------

    def test_live_returns_same_dict_object(self):
        c = self._loaded_client("live")
        payload = c._build_callback_payload()
        assert payload is c.candle_deques

    def test_live_deques_are_same_objects(self):
        c = self._loaded_client("live")
        payload = c._build_callback_payload()
        assert payload["1m"] is c.candle_deques["1m"]

    # -- deque_copy mode -----------------------------------------------------

    def test_deque_copy_returns_new_dict(self):
        c = self._loaded_client("deque_copy")
        payload = c._build_callback_payload()
        assert payload is not c.candle_deques

    def test_deque_copy_deques_are_new_containers(self):
        c = self._loaded_client("deque_copy")
        payload = c._build_callback_payload()
        assert payload["1m"] is not c.candle_deques["1m"]

    def test_deque_copy_elements_are_shared(self):
        c = self._loaded_client("deque_copy")
        payload = c._build_callback_payload()
        # Elements should be the same dict objects (not deep-copied)
        for iv in ["1m", "5m"]:
            for orig, copied in zip(c.candle_deques[iv], payload[iv]):
                assert orig is copied

    def test_deque_copy_maxlen_preserved(self):
        c = _make_client(intervals=["1m"], callback_snapshot_mode="deque_copy", candle_deque_maxlen=50)
        payload = c._build_callback_payload()
        assert payload["1m"].maxlen == 50

    def test_deque_copy_contains_same_data(self):
        c = self._loaded_client("deque_copy")
        payload = c._build_callback_payload()
        assert list(payload["1m"]) == list(c.candle_deques["1m"])

    # -- deep_copy mode ------------------------------------------------------

    def test_deep_copy_returns_new_dict(self):
        c = self._loaded_client("deep_copy")
        payload = c._build_callback_payload()
        assert payload is not c.candle_deques

    def test_deep_copy_deques_are_new_containers(self):
        c = self._loaded_client("deep_copy")
        payload = c._build_callback_payload()
        assert payload["1m"] is not c.candle_deques["1m"]

    def test_deep_copy_elements_are_independent(self):
        c = self._loaded_client("deep_copy")
        payload = c._build_callback_payload()
        for iv in ["1m", "5m"]:
            for orig, copied in zip(c.candle_deques[iv], payload[iv]):
                # Must be different dict objects (deep copied)
                assert orig is not copied
                # But data must be equal
                assert orig == copied

    def test_deep_copy_mutation_does_not_affect_source(self):
        c = self._loaded_client("deep_copy")
        payload = c._build_callback_payload()
        original_ts = list(c.candle_deques["1m"])[0]["ts"]
        # Mutate the payload copy
        list(payload["1m"])[0]["ts"] = 9999
        # Source deque should be unchanged
        assert list(c.candle_deques["1m"])[0]["ts"] == original_ts

    # -- lock released after build -------------------------------------------

    def test_storage_lock_released_after_build(self):
        """_build_callback_payload must release the storage lock when it returns."""
        for mode in ("live", "deque_copy", "deep_copy"):
            c = self._loaded_client(mode)
            c._build_callback_payload()
            # If the lock is released, we can acquire it immediately
            acquired = c.storage_lock.acquire(blocking=False)
            assert acquired, f"storage_lock still held after build in mode={mode!r}"
            c.storage_lock.release()


# ===========================================================================
# 4. _handle_candle_event filtering
# ===========================================================================

class TestHandleCandleEventFiltering:
    """Events that don't match the client's exchange/symbol/interval must be
    silently ignored without touching the deques or setting data_updated_event."""

    def _client(self) -> ZMQClient:
        return _make_client(intervals=["1m", "5m"])

    def _event_sets_updated(self, client: ZMQClient, topic: str, payload: dict) -> bool:
        client.data_updated_event.clear()
        client._handle_candle_event(topic, payload)
        return client.data_updated_event.is_set()

    def test_wrong_exchange_ignored(self):
        c = self._client()
        topic = "BINANCE:CANDLE:KRW-BTC:1m:UPDATE"
        payload = {"candle": _make_candle(1)}
        updated = self._event_sets_updated(c, topic, payload)
        assert not updated
        assert len(c.candle_deques["1m"]) == 0

    def test_wrong_symbol_ignored(self):
        c = self._client()
        topic = "UPBIT:CANDLE:KRW-ETH:1m:UPDATE"
        payload = {"candle": _make_candle(1)}
        updated = self._event_sets_updated(c, topic, payload)
        assert not updated
        assert len(c.candle_deques["1m"]) == 0

    def test_unsubscribed_interval_ignored(self):
        c = self._client()
        topic = "UPBIT:CANDLE:KRW-BTC:1h:UPDATE"
        payload = {"candle": _make_candle(1)}
        updated = self._event_sets_updated(c, topic, payload)
        assert not updated

    def test_malformed_topic_ignored(self):
        c = self._client()
        updated = self._event_sets_updated(c, "UPBIT:CANDLE:KRW-BTC", {"candle": _make_candle(1)})
        assert not updated

    def test_extra_topic_segments_ignored(self):
        c = self._client()
        topic = "UPBIT:CANDLE:KRW-BTC:1m:UPDATE:EXTRA"
        updated = self._event_sets_updated(c, topic, {"candle": _make_candle(1)})
        assert not updated
        assert len(c.candle_deques["1m"]) == 0

    def test_wrong_channel_ignored(self):
        c = self._client()
        topic = "UPBIT:TICKER:KRW-BTC:1m:UPDATE"
        updated = self._event_sets_updated(c, topic, {"candle": _make_candle(1)})
        assert not updated

    @pytest.mark.parametrize("payload", [None, [], "not-a-dict"])
    def test_non_dict_payload_ignored(self, payload):
        c = self._client()
        topic = "UPBIT:CANDLE:KRW-BTC:1m:UPDATE"
        updated = self._event_sets_updated(c, topic, payload)
        assert not updated
        assert len(c.candle_deques["1m"]) == 0

    def test_exchange_case_insensitive_init(self):
        """exchange param is lowered; prefix becomes uppercase for filtering."""
        c = _make_client(intervals=["1m"], exchange="UPBIT")
        # Sending UPBIT-prefixed topic should be accepted
        _inject_update(c, "1m", _make_candle(1))
        assert len(c.candle_deques["1m"]) == 1


# ===========================================================================
# 5. _handle_candle_event UPDATE semantics
# ===========================================================================

class TestHandleCandleEventUpdate:
    def test_update_into_empty_deque_appends(self):
        c = _make_client(intervals=["1m"])
        candle = _make_candle(1)
        _inject_update(c, "1m", candle)
        assert len(c.candle_deques["1m"]) == 1
        assert c.candle_deques["1m"][-1] == candle

    def test_update_replaces_last_element(self):
        c = _make_client(intervals=["1m"])
        c.candle_deques["1m"].append(_make_candle(1, close=50.0))
        new_candle = _make_candle(1, close=99.0)
        _inject_update(c, "1m", new_candle)
        assert len(c.candle_deques["1m"]) == 1
        assert c.candle_deques["1m"][-1]["close"] == 99.0

    def test_update_sets_data_updated_event(self):
        c = _make_client(intervals=["1m"])
        c.data_updated_event.clear()
        _inject_update(c, "1m", _make_candle(1))
        assert c.data_updated_event.is_set()

    def test_update_identical_candle_does_not_set_data_updated_event(self):
        c = _make_client(intervals=["1m"])
        candle = _make_candle(1, close=100.0)
        c.candle_deques["1m"].append(dict(candle))
        c.data_updated_event.clear()
        _inject_update(c, "1m", dict(candle))
        assert not c.data_updated_event.is_set()
        assert list(c.candle_deques["1m"]) == [candle]

    def test_update_does_not_set_event_when_zero_capacity_stores_nothing(self):
        c = _make_client(intervals=["1m"], candle_deque_maxlen=0)

        _inject_update(c, "1m", _make_candle(1))

        assert list(c.candle_deques["1m"]) == []
        assert not c.data_updated_event.is_set()

    def test_update_missing_candle_key_ignored(self):
        c = _make_client(intervals=["1m"])
        c.data_updated_event.clear()
        topic = "UPBIT:CANDLE:KRW-BTC:1m:UPDATE"
        c._handle_candle_event(topic, {})  # no 'candle' key
        assert not c.data_updated_event.is_set()
        assert len(c.candle_deques["1m"]) == 0

    def test_update_non_dict_candle_ignored(self):
        c = _make_client(intervals=["1m"])
        existing = _make_candle(1, close=50.0)
        c.candle_deques["1m"].append(existing)
        c.data_updated_event.clear()
        topic = "UPBIT:CANDLE:KRW-BTC:1m:UPDATE"
        c._handle_candle_event(topic, {"candle": []})
        assert not c.data_updated_event.is_set()
        assert len(c.candle_deques["1m"]) == 1
        assert c.candle_deques["1m"][-1] == existing

    def test_update_only_affects_target_interval(self):
        c = _make_client(intervals=["1m", "5m"])
        _inject_update(c, "1m", _make_candle(1))
        assert len(c.candle_deques["5m"]) == 0


# ===========================================================================
# 6. _handle_candle_event CLOSE semantics
# ===========================================================================

class TestHandleCandleEventClose:
    def test_close_updates_last_and_appends_new(self):
        c = _make_client(intervals=["1m"])
        c.candle_deques["1m"].append(_make_candle(1, close=50.0))
        closed = _make_candle(1, close=55.0)
        new = _make_candle(2, close=60.0)
        _inject_close(c, "1m", closed, new)
        assert len(c.candle_deques["1m"]) == 2
        assert c.candle_deques["1m"][-2]["close"] == 55.0
        assert c.candle_deques["1m"][-1]["close"] == 60.0

    def test_close_into_empty_deque_appends_both(self):
        c = _make_client(intervals=["1m"])
        closed = _make_candle(1)
        new = _make_candle(2)
        _inject_close(c, "1m", closed, new)
        assert len(c.candle_deques["1m"]) == 2

    def test_close_missing_candle_key_ignored(self):
        c = _make_client(intervals=["1m"])
        c.data_updated_event.clear()
        topic = "UPBIT:CANDLE:KRW-BTC:1m:CLOSE"
        c._handle_candle_event(topic, {"new": _make_candle(2)})  # no 'candle'
        assert not c.data_updated_event.is_set()

    def test_close_missing_new_key_ignored(self):
        c = _make_client(intervals=["1m"])
        c.data_updated_event.clear()
        topic = "UPBIT:CANDLE:KRW-BTC:1m:CLOSE"
        c._handle_candle_event(topic, {"candle": _make_candle(1)})  # no 'new'
        assert not c.data_updated_event.is_set()

    @pytest.mark.parametrize(
        "payload",
        [
            {"candle": [], "new": _make_candle(2)},
            {"candle": _make_candle(1), "new": []},
            {"candle": [], "new": []},
        ],
    )
    def test_close_non_dict_candle_or_new_ignored(self, payload):
        c = _make_client(intervals=["1m"])
        existing = _make_candle(1, close=50.0)
        c.candle_deques["1m"].append(existing)
        c.data_updated_event.clear()
        topic = "UPBIT:CANDLE:KRW-BTC:1m:CLOSE"
        c._handle_candle_event(topic, payload)
        assert not c.data_updated_event.is_set()
        assert list(c.candle_deques["1m"]) == [existing]

    def test_close_sets_data_updated_event(self):
        c = _make_client(intervals=["1m"])
        c.data_updated_event.clear()
        _inject_close(c, "1m", _make_candle(1), _make_candle(2))
        assert c.data_updated_event.is_set()

    def test_close_does_not_set_event_when_bounded_storage_is_unchanged(self):
        c = _make_client(intervals=["1m"], candle_deque_maxlen=1)
        candle = _make_candle(1)
        c.candle_deques["1m"].append(candle)

        _inject_close(c, "1m", dict(candle), dict(candle))

        assert list(c.candle_deques["1m"]) == [candle]
        assert not c.data_updated_event.is_set()


# ===========================================================================
# 7. _handle_candle_event RECONCILE semantics
# ===========================================================================

class TestHandleCandleEventReconcile:
    def test_reconcile_replaces_matching_ts(self):
        c = _make_client(intervals=["1m"])
        c.candle_deques["1m"].extend([_make_candle(i) for i in range(5)])
        patched = _make_candle(2, close=999.0)
        _inject_reconcile(c, "1m", patched)
        assert c.candle_deques["1m"][2]["close"] == 999.0

    def test_reconcile_no_match_leaves_deque_unchanged(self):
        c = _make_client(intervals=["1m"])
        original = [_make_candle(i) for i in range(3)]
        c.candle_deques["1m"].extend(original)
        _inject_reconcile(c, "1m", _make_candle(99, close=1.0))
        # All ts values should be unchanged
        assert all(c.candle_deques["1m"][i]["ts"] == i for i in range(3))

    def test_reconcile_no_match_does_not_set_data_updated_event(self):
        c = _make_client(intervals=["1m"])
        c.candle_deques["1m"].append(_make_candle(1))
        c.data_updated_event.clear()
        _inject_reconcile(c, "1m", _make_candle(99, close=1.0))
        assert not c.data_updated_event.is_set()

    def test_reconcile_identical_candle_does_not_set_data_updated_event(self):
        c = _make_client(intervals=["1m"])
        candle = _make_candle(1, close=100.0)
        c.candle_deques["1m"].append(dict(candle))
        c.data_updated_event.clear()
        _inject_reconcile(c, "1m", dict(candle))
        assert not c.data_updated_event.is_set()
        assert list(c.candle_deques["1m"]) == [candle]

    def test_reconcile_missing_candle_key_ignored(self):
        c = _make_client(intervals=["1m"])
        c.data_updated_event.clear()
        topic = "UPBIT:CANDLE:KRW-BTC:1m:RECONCILE"
        c._handle_candle_event(topic, {})
        assert not c.data_updated_event.is_set()

    def test_reconcile_non_dict_candle_ignored(self):
        c = _make_client(intervals=["1m"])
        existing = _make_candle(1, close=50.0)
        c.candle_deques["1m"].append(existing)
        c.data_updated_event.clear()
        topic = "UPBIT:CANDLE:KRW-BTC:1m:RECONCILE"
        c._handle_candle_event(topic, {"candle": []})
        assert not c.data_updated_event.is_set()
        assert list(c.candle_deques["1m"]) == [existing]

    def test_reconcile_missing_ts_key_ignored(self):
        c = _make_client(intervals=["1m"])
        c.candle_deques["1m"].append(_make_candle(1))
        c.data_updated_event.clear()
        topic = "UPBIT:CANDLE:KRW-BTC:1m:RECONCILE"
        c._handle_candle_event(topic, {"candle": {"close": 1.0}})  # no 'ts'
        assert not c.data_updated_event.is_set()

    def test_reconcile_sets_data_updated_event(self):
        c = _make_client(intervals=["1m"])
        c.candle_deques["1m"].append(_make_candle(1))
        c.data_updated_event.clear()
        _inject_reconcile(c, "1m", _make_candle(1, close=200.0))
        assert c.data_updated_event.is_set()

    def test_reconcile_only_replaces_last_matching(self):
        """If the same ts appears more than once, only one entry is patched
        (the search is from the tail, and breaks after first match)."""
        c = _make_client(intervals=["1m"])
        # Add duplicate ts entries
        c.candle_deques["1m"].extend([_make_candle(1, close=10.0), _make_candle(1, close=20.0)])
        _inject_reconcile(c, "1m", _make_candle(1, close=99.0))
        values = [d["close"] for d in c.candle_deques["1m"]]
        # Only the last occurrence (index 1) should be patched
        assert values == [10.0, 99.0]

    def test_unknown_event_type_is_ignored(self):
        c = _make_client(intervals=["1m"])
        c.candle_deques["1m"].append(_make_candle(1, close=10.0))
        c.data_updated_event.clear()
        topic = "UPBIT:CANDLE:KRW-BTC:1m:SNAPSHOT"
        c._handle_candle_event(topic, {"candle": _make_candle(1, close=99.0)})
        assert c.candle_deques["1m"][-1]["close"] == 10.0
        assert not c.data_updated_event.is_set()


# ===========================================================================
# 8. Callback runs outside the storage lock
# ===========================================================================

class TestCallbackOutsideLock:
    """Verify that the callback payload is built and the lock is released
    before the user callback is invoked (i.e., the lock is not held during
    the callback call in _strategy_trigger_thread)."""

    def test_lock_not_held_inside_callback_for_deque_copy(self):
        """The callback receives a copy, so the live lock is not relevant.
        We verify that the storage lock is acquirable from inside the callback."""
        lock_acquirable_inside_callback: list[bool] = []

        def cb(payload):
            # Try to acquire storage_lock without blocking
            result = client.storage_lock.acquire(blocking=False)
            lock_acquirable_inside_callback.append(result)
            if result:
                client.storage_lock.release()

        client = _make_client(intervals=["1m"], callback=cb, callback_snapshot_mode="deque_copy")
        client.candle_deques["1m"].append(_make_candle(1))

        # Simulate what _strategy_trigger_thread does for each callback cycle
        payload = client._build_callback_payload()
        client.candle_handler_callback(payload)

        assert lock_acquirable_inside_callback == [True], (
            "storage_lock was held during callback execution"
        )


# ===========================================================================
# 9. Stop and thread cleanup
# ===========================================================================

class TestStopAndThreadCleanup:
    """Tests for the stop() lifecycle without a real gateway.

    We patch _send_request to return None quickly so unsubscribe calls
    don't block.
    """

    def _start_threads_directly(self, client: ZMQClient) -> None:
        """Start only the data_listener and strategy_trigger threads.
        Skip renewer (it would call _send_request in a real loop).
        This avoids needing a gateway while still testing thread lifecycle.
        """
        with patch.object(client, "_data_listener_thread", client._data_listener_thread), \
             patch("sk_zmq.client.zmq.Context"):
            listener = threading.Thread(target=_noop_listener, args=(client,), name="ZMQListener", daemon=True)
            trigger = threading.Thread(target=client._strategy_trigger_thread, name="StrategyTrigger", daemon=True)
            client.threads.extend([listener, trigger])
            listener.start()
            trigger.start()

    def test_stop_sets_stop_event(self):
        client = _make_client()
        with patch.object(client, "_send_request", return_value=None):
            client.stop()
        assert client.stop_event.is_set()

    def test_stop_joins_threads(self):
        """All threads registered in client.threads are joined (not alive) after stop()."""
        client = _make_client(intervals=["1m"], throttle_seconds=0.05)
        self._start_threads_directly(client)

        time.sleep(0.05)  # let threads start

        with patch.object(client, "_send_request", return_value=None):
            client.stop()

        for t in client.threads:
            assert not t.is_alive(), f"Thread {t.name!r} still alive after stop()"

    def test_stop_is_idempotent_for_stop_event(self):
        """Calling stop() a second time must not raise and stop_event stays set.

        Use a real zmq.Context so the first stop can terminate it, then verify
        second stop does not re-enter network request flow or terminate again.
        """
        client = ZMQClient(
            intervals=["1m"],
            candle_handler_callback=lambda _d: None,
            **_COMMON_KWARGS,
        )
        with patch.object(client, "_send_request", return_value=None) as send_request:
            client.stop()
            client.stop()  # second call — must not raise
            assert client.context.closed
        assert client.stop_event.is_set()
        assert client.context.closed
        assert send_request.call_count == 1

    def test_stop_without_started_threads(self):
        """stop() with no threads in client.threads must not raise."""
        client = _make_client()
        assert client.threads == []
        with patch.object(client, "_send_request", return_value=None):
            client.stop()
        assert client.stop_event.is_set()

    def test_stop_completes_cleanup_when_unsubscribe_raises(self):
        client = _make_client(intervals=["1m"])

        with patch.object(client, "_send_request", side_effect=RuntimeError("failed")):
            client.stop()

        assert client.stop_event.is_set()
        client.context.term.assert_called_once_with()

    def test_stop_uses_bounded_unsubscribe_request_budget(self):
        client = _make_client(intervals=["1m", "5m"])

        with patch.object(client, "_send_request", return_value=None) as send_request:
            client.stop()

        assert send_request.call_count == 2
        for call in send_request.call_args_list:
            assert call.kwargs == {
                "max_retries": 1,
                "receive_timeout_ms": client._STOP_UNSUBSCRIBE_TIMEOUT_MS,
            }
        client.context.term.assert_called_once_with()

    def test_stop_skips_joining_the_calling_registered_thread(self):
        client = _make_client(intervals=["1m"])
        client.threads.append(threading.current_thread())

        with patch.object(client, "_send_request", return_value=None):
            client.stop()

        assert client.stop_event.is_set()
        client.context.term.assert_called_once_with()


def _noop_listener(client: ZMQClient) -> None:
    """A stand-in for _data_listener_thread that just waits for stop_event."""
    client.stop_event.wait()


class _InvalidJsonThenStopSocket:
    def __init__(self, client: ZMQClient):
        self.client = client
        self.closed = False

    def connect(self, _endpoint):
        pass

    def setsockopt_string(self, _option, _value):
        pass

    def recv_multipart(self, flags=0):
        self.client.stop_event.set()
        return [b"UPBIT:CANDLE:KRW-BTC:1m:UPDATE", b"{"]

    def close(self):
        self.closed = True


class _MalformedMultipartThenStopSocket:
    def __init__(self, client: ZMQClient, frames):
        self.client = client
        self.frames = frames
        self.closed = False

    def connect(self, _endpoint):
        pass

    def setsockopt_string(self, _option, _value):
        pass

    def recv_multipart(self, flags=0):
        self.client.stop_event.set()
        return self.frames

    def close(self):
        self.closed = True


class _NonDictResponseSocket:
    def __init__(self):
        self.closed = False

    def setsockopt(self, _option, _value):
        pass

    def connect(self, _endpoint):
        pass

    def send(self, _payload):
        pass

    def recv(self):
        return b'["bad"]'

    def close(self):
        self.closed = True


class TestDataListenerMalformedPayloads:
    def test_invalid_json_payload_is_ignored_without_crashing_listener(self):
        client = _make_client(intervals=["1m"])
        socket = _InvalidJsonThenStopSocket(client)
        client.context.socket.return_value = socket

        client._data_listener_thread()

        assert socket.closed
        assert len(client.candle_deques["1m"]) == 0
        assert not client.data_updated_event.is_set()

    @pytest.mark.parametrize(
        "frames",
        [
            [b"UPBIT:CANDLE:KRW-BTC:1m:UPDATE"],
            [b"UPBIT:CANDLE:KRW-BTC:1m:UPDATE", b'{"candle": {"ts": 1}}', b"extra"],
        ],
    )
    def test_malformed_multipart_payload_is_ignored_without_crashing_listener(self, frames):
        client = _make_client(intervals=["1m"])
        socket = _MalformedMultipartThenStopSocket(client, frames)
        client.context.socket.return_value = socket

        client._data_listener_thread()

        assert socket.closed
        assert len(client.candle_deques["1m"]) == 0
        assert not client.data_updated_event.is_set()


# ===========================================================================
# 10. _strategy_trigger_thread callback invocation
# ===========================================================================

class TestStrategyTriggerThread:
    """Verify the trigger thread calls the callback when data_updated_event fires."""

    def test_throttled_callback_called_on_update(self):
        call_count = [0]

        def cb(payload):
            call_count[0] += 1

        client = _make_client(intervals=["1m"], callback=cb, throttle_seconds=0.05)

        trigger = threading.Thread(
            target=client._strategy_trigger_thread,
            name="StrategyTrigger",
            daemon=True,
        )
        trigger.start()

        # Inject an update to trigger callback
        _inject_update(client, "1m", _make_candle(1))
        time.sleep(0.2)  # wait for at least one throttle cycle

        client.stop_event.set()
        trigger.join(timeout=2)

        assert call_count[0] >= 1

    def test_unthrottled_callback_called_on_update(self):
        call_count = [0]

        def cb(payload):
            call_count[0] += 1

        client = _make_client(intervals=["1m"], callback=cb, throttle_seconds=None)

        trigger = threading.Thread(
            target=client._strategy_trigger_thread,
            name="StrategyTrigger",
            daemon=True,
        )
        trigger.start()

        _inject_update(client, "1m", _make_candle(1))
        time.sleep(0.2)

        client.stop_event.set()
        client.data_updated_event.set()  # unblock the wait(timeout=1) if needed
        trigger.join(timeout=2)

        assert call_count[0] >= 1

    def test_callback_exception_does_not_kill_trigger_thread(self):
        """An exception in the callback must not crash the trigger thread."""
        def bad_cb(payload):
            raise RuntimeError("intentional test error")

        client = _make_client(intervals=["1m"], callback=bad_cb, throttle_seconds=0.05)

        trigger = threading.Thread(
            target=client._strategy_trigger_thread,
            name="StrategyTrigger",
            daemon=True,
        )
        trigger.start()

        # Fire two updates; the thread should survive both
        _inject_update(client, "1m", _make_candle(1))
        time.sleep(0.15)
        _inject_update(client, "1m", _make_candle(2))
        time.sleep(0.15)

        assert trigger.is_alive(), "Trigger thread died after callback exception"

        client.stop_event.set()
        trigger.join(timeout=2)


# ===========================================================================
# 11. Start + renewal lifecycle
# ===========================================================================


class _FiniteWait:
    def __init__(self, cycles: int):
        self.cycles = cycles
        self.calls = 0

    def __call__(self, timeout=None) -> bool:
        self.calls += 1
        return self.calls > self.cycles


class _RecordingWait:
    def __init__(self):
        self.timeouts = []

    def __call__(self, timeout=None) -> bool:
        self.timeouts.append(timeout)
        return True


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


class TestLifecycleStartAndRenewal:
    """Lifecycle tests around start() and subscription renewal behavior."""

    @pytest.mark.parametrize("server_candle_ttl", [0, -1])
    def test_non_positive_server_candle_ttl_raises(self, server_candle_ttl):
        with pytest.raises(ValueError, match="server_candle_ttl"):
            _make_client(server_candle_ttl=server_candle_ttl)

    def test_short_ttl_schedules_renewal_before_expiration(self):
        client = _make_client(server_candle_ttl=5)
        wait = _RecordingWait()

        with patch.object(type(client.stop_event), "wait", wait):
            client._subscription_renewer_thread()

        assert len(wait.timeouts) == 1
        assert 0 < wait.timeouts[0] < client.server_candle_ttl

    def test_start_sends_initial_snapshot_requests_for_all_intervals(self):
        client = _make_client(intervals=["1m", "5m"], candle_deque_maxlen=4)
        responses = [
            {"status": "ok", "data": [_make_candle(1)]},
            {"status": "ok", "data": [_make_candle(2)]},
        ]

        with patch.object(client, "_send_request", side_effect=responses) as send_request, \
            patch.object(client, "_data_listener_thread", return_value=None), \
            patch.object(client, "_strategy_trigger_thread", return_value=None), \
            patch.object(client, "_subscription_renewer_thread", return_value=None):
            assert client.start() is True

        for t in client.threads:
            t.join(timeout=1)

        assert [call.args[0] for call in send_request.call_args_list] == [
            {
                "action": "subscribe_candle",
                "symbol": "KRW-BTC",
                "interval": "1m",
                "history_count": 4,
                "exchange": "upbit",
            },
            {
                "action": "subscribe_candle",
                "symbol": "KRW-BTC",
                "interval": "5m",
                "history_count": 4,
                "exchange": "upbit",
            },
        ]
        assert len(client.threads) == 3

    @pytest.mark.parametrize(
        "snapshot_response",
        [
            None,
            {"status": "ok", "data": []},
            {"status": "error", "data": [_make_candle(1)]},
        ],
    )
    def test_start_returns_false_and_starts_no_threads_when_snapshot_fails_or_empty(self, snapshot_response):
        client = _make_client(intervals=["1m"])
        with patch.object(client, "_send_request", return_value=snapshot_response) as send_request:
            assert client.start() is False
        assert send_request.call_count == 1
        assert client.threads == []

    @pytest.mark.parametrize("snapshot_response", [["bad"], "bad", 1])
    def test_start_returns_false_and_starts_no_threads_when_snapshot_response_is_not_dict(self, snapshot_response):
        client = _make_client(intervals=["1m"])
        with patch.object(client, "_send_request", return_value=snapshot_response) as send_request:
            assert client.start() is False
        assert send_request.call_count == 1
        assert client.threads == []

    @pytest.mark.parametrize("snapshot_data", ["bad", {"ts": 1}, ["bad"], [None]])
    def test_start_rejects_malformed_snapshot_data(self, snapshot_data):
        client = _make_client(intervals=["1m"])
        response = {"status": "ok", "data": snapshot_data}

        with patch.object(client, "_send_request", return_value=response) as send_request:
            assert client.start() is False

        assert send_request.call_count == 1
        assert client.threads == []
        assert list(client.candle_deques["1m"]) == []

    def test_start_failure_leaves_no_partial_snapshot_before_retry(self):
        client = _make_client(intervals=["1m", "5m"])
        first_snapshot = _make_candle(1)
        responses = [
            {"status": "ok", "data": [first_snapshot]},
            {"status": "error", "data": []},
            {"status": "ok", "data": [first_snapshot]},
            {"status": "ok", "data": [_make_candle(2)]},
        ]

        with patch.object(client, "_send_request", side_effect=responses), \
            patch.object(client, "_data_listener_thread", return_value=None), \
            patch.object(client, "_strategy_trigger_thread", return_value=None), \
            patch.object(client, "_subscription_renewer_thread", return_value=None):
            assert client.start() is False
            assert list(client.candle_deques["1m"]) == []
            assert list(client.candle_deques["5m"]) == []

            assert client.start() is True

        for thread in client.threads:
            thread.join(timeout=1)

        assert list(client.candle_deques["1m"]) == [first_snapshot]
        assert list(client.candle_deques["5m"]) == [_make_candle(2)]

    def test_send_request_rejects_decoded_non_dict_response(self):
        client = _make_client(intervals=["1m"])
        socket = _NonDictResponseSocket()
        client.context.socket.return_value = socket

        assert client._send_request({"action": "subscribe_candle"}, max_retries=1) is None
        assert socket.closed

    def test_send_request_handles_socket_creation_failure(self):
        client = _make_client(intervals=["1m"])
        client.context.socket.side_effect = zmq.ZMQError("socket setup failed")

        assert client._send_request({"action": "subscribe_candle"}, max_retries=1) is None

    def test_send_request_closes_socket_when_connect_fails(self):
        client = _make_client(intervals=["1m"])
        socket = MagicMock()
        socket.connect.side_effect = zmq.ZMQError("connect failed")
        client.context.socket.return_value = socket

        assert client._send_request({"action": "subscribe_candle"}, max_retries=1) is None
        socket.close.assert_called_once_with()

    def test_send_request_uses_custom_receive_timeout(self):
        client = _make_client(intervals=["1m"])
        socket = MagicMock()
        socket.recv.side_effect = zmq.Again()
        client.context.socket.return_value = socket

        assert client._send_request(
            {"action": "unsubscribe_candle"},
            max_retries=1,
            receive_timeout_ms=250,
        ) is None
        socket.setsockopt.assert_any_call(zmq.RCVTIMEO, 250)

    def test_send_request_stops_retrying_when_shutdown_is_requested(self):
        client = _make_client(intervals=["1m"])
        socket = MagicMock()
        socket.recv.side_effect = zmq.Again()
        client.context.socket.return_value = socket
        client.stop_event.set()

        with patch("sk_zmq.client.time.sleep") as sleep:
            assert client._send_request({"action": "subscribe_candle"}) is None

        assert socket.send.call_count == 1
        assert sleep.call_count == 0

    def test_send_request_interrupts_retry_backoff_on_shutdown(self):
        client = _make_client(intervals=["1m"])
        socket = MagicMock()
        socket.recv.side_effect = zmq.Again()
        client.context.socket.return_value = socket
        stop_event = _StopOnWait()
        client.stop_event = stop_event

        with patch("sk_zmq.client.time.sleep"):
            assert client._send_request({"action": "subscribe_candle"}) is None

        assert socket.send.call_count == 1
        assert stop_event.timeouts == [2]

    def test_start_returns_false_when_request_socket_setup_fails(self):
        client = _make_client(intervals=["1m"])
        client.context.socket.side_effect = zmq.ZMQError("socket setup failed")

        with patch.object(type(client.stop_event), "wait", return_value=False):
            assert client.start() is False

        assert client.context.socket.call_count == 5
        assert client.threads == []

    def test_renewer_counts_truthy_non_dict_response_as_failure(self):
        client = _make_client(intervals=["1m"])

        with patch.object(client, "_send_request", return_value=["bad"]), \
            patch.object(type(client.stop_event), "wait", _FiniteWait(1)):
            client._subscription_renewer_thread()

        assert client.consecutive_renewal_failures == 1

    def test_renewer_failures_increase_and_trigger_critical_callback(self):
        critical_calls = []
        client = _make_client(intervals=["1m"], on_critical=critical_calls.append)

        with patch.object(client, "_send_request", return_value={"status": "error"}), \
            patch.object(type(client.stop_event), "wait", _FiniteWait(client.MAX_CONSECUTIVE_FAILURES)):
            client._subscription_renewer_thread()

        assert client.consecutive_renewal_failures == client.MAX_CONSECUTIVE_FAILURES
        assert len(critical_calls) == 1

    def test_successful_renewal_after_failures_resets_counter(self):
        responses = iter([
            {"status": "error"},
            {"status": "error"},
            {"status": "ok"},
        ])
        client = _make_client(intervals=["1m"])

        def pop_response(*_args, **_kwargs):
            return next(responses)

        with patch.object(client, "_send_request", side_effect=pop_response), \
            patch.object(type(client.stop_event), "wait", _FiniteWait(3)):
            client._subscription_renewer_thread()

        assert client.consecutive_renewal_failures == 0

    def test_renewal_sends_history_count_1(self):
        """Renewal thread must send history_count=1 (TTL extend only)."""
        client = _make_client(intervals=["1m", "5m"])

        renew_responses = [
            {"status": "ok", "data": [_make_candle(1)]},
            {"status": "ok", "data": [_make_candle(2)]},
            {"status": "ok", "data": [_make_candle(1)]},
            {"status": "ok", "data": [_make_candle(2)]},
            {"status": "ok", "data": [_make_candle(1)]},
            {"status": "ok", "data": [_make_candle(2)]},
        ]

        with patch.object(client, "_send_request", side_effect=renew_responses) as send_request, \
             patch.object(type(client.stop_event), "wait", _FiniteWait(3)):
            client._subscription_renewer_thread()

        assert send_request.call_count == 6
        for call in send_request.call_args_list:
            req = call.args[0]
            assert req["action"] == "subscribe_candle"
            assert req["history_count"] == 1

    def test_normal_renewal_start_is_debug_log(self, caplog):
        client = _make_client(intervals=["1m"])

        with caplog.at_level(logging.DEBUG, logger="sk_zmq.client"), \
            patch.object(client, "_send_request", return_value={"status": "ok"}), \
            patch.object(type(client.stop_event), "wait", _FiniteWait(1)):
            client._subscription_renewer_thread()

        renewal_records = [
            record for record in caplog.records
            if record.getMessage() == "모든 캔들 구독 갱신을 시작합니다..."
        ]
        assert len(renewal_records) == 1
        assert renewal_records[0].levelno == logging.DEBUG

    def test_stop_sends_unsubscribe_for_each_interval(self):
        """stop() must send unsubscribe_candle for each subscribed interval."""
        client = _make_client(intervals=["1m", "5m"])

        with patch.object(client, "_send_request", return_value=None) as send_request:
            client.stop()

        assert send_request.call_count == 2
        expected_actions = {"unsubscribe_candle", "unsubscribe_candle"}
        actual_actions = {call.args[0]["action"] for call in send_request.call_args_list}
        assert actual_actions == expected_actions

        unsubscribe_calls = [call.args[0] for call in send_request.call_args_list]
        intervals_sent = {c["interval"] for c in unsubscribe_calls}
        assert intervals_sent == {"1m", "5m"}

        for call in unsubscribe_calls:
            assert call["symbol"] == "KRW-BTC"
            assert call["exchange"] == "upbit"

    def test_stop_unsubscribes_after_in_flight_renewal(self):
        critical_calls = []
        client = _make_client(
            intervals=["1m", "5m"],
            on_critical=critical_calls.append,
        )
        real_wait = client.stop_event.wait
        first_wait = True
        first_renewal_started = threading.Event()
        release_first_renewal = threading.Event()
        both_unsubscribed = threading.Event()
        calls = []
        calls_lock = threading.Lock()

        def enter_one_renewal_cycle(timeout=None):
            nonlocal first_wait
            if first_wait:
                first_wait = False
                return False
            return real_wait(timeout)

        def send_request(request, **_kwargs):
            action = request["action"]
            interval = request["interval"]
            if action == "subscribe_candle" and interval == "1m":
                first_renewal_started.set()
                assert release_first_renewal.wait(timeout=2)
            with calls_lock:
                calls.append((action, interval))
                unsubscribe_count = sum(
                    recorded_action == "unsubscribe_candle"
                    for recorded_action, _ in calls
                )
                if unsubscribe_count == 2:
                    both_unsubscribed.set()
            return {"status": "ok"}

        with patch.object(client.stop_event, "wait", side_effect=enter_one_renewal_cycle), \
            patch.object(client, "_send_request", side_effect=send_request):
            renewer = threading.Thread(target=client._subscription_renewer_thread)
            renewer.start()
            assert first_renewal_started.wait(timeout=1)

            stopper = threading.Thread(target=client.stop)
            stopper.start()
            both_unsubscribed.wait(timeout=0.25)
            release_first_renewal.set()

            stopper.join(timeout=2)
            renewer.join(timeout=2)

        assert not stopper.is_alive()
        assert not renewer.is_alive()
        assert calls == [
            ("subscribe_candle", "1m"),
            ("unsubscribe_candle", "1m"),
            ("unsubscribe_candle", "5m"),
        ]
        assert client.consecutive_renewal_failures == 0
        assert critical_calls == []
