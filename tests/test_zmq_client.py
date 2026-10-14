"""Core candle data integrity, callback and lifecycle behavior tests."""

from __future__ import annotations

import logging

import threading

import time

from unittest.mock import MagicMock, patch

import pytest

import zmq

from sk_zmq.client import (
    GatewayRequestCode,
    GatewayRequestResult,
    ZMQClient,
)

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
    return {
        "ts": ts,
        "open": 100.0,
        "high": 105.0,
        "low": 95.0,
        "close": close,
        "volume": 1.0,
    }


STREAM = "a" * 32
RESTART = "b" * 32


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
    """Drive _handle_candle_event with an UPDATE payload directly."""
    topic = f"UPBIT:CANDLE:{client.symbol}:{interval}:UPDATE"
    client._handle_candle_event(topic, _event_payload(client, candle=candle))


def _inject_close(
    client: ZMQClient, interval: str, closed_candle: dict, new_candle: dict
) -> None:
    topic = f"UPBIT:CANDLE:{client.symbol}:{interval}:CLOSE"
    client._handle_candle_event(
        topic, _event_payload(client, candle=closed_candle, new=new_candle)
    )


def _inject_reconcile(client: ZMQClient, interval: str, candle: dict) -> None:
    topic = f"UPBIT:CANDLE:{client.symbol}:{interval}:RECONCILE"
    client._handle_candle_event(topic, _event_payload(client, candle=candle))


def _event_payload(client, **fields):
    client._test_seq = getattr(client, "_test_seq", 0) + 1
    return dict(fields, stream_id=STREAM, seq=client._test_seq)


class TestStopAndThreadCleanup:
    def test_stop_waits_for_in_flight_callback_before_returning(self):
        callback_entered = threading.Event()
        release_callback = threading.Event()

        def callback(_payload):
            callback_entered.set()
            assert release_callback.wait(timeout=2)

        client = _make_client(
            intervals=["1m"], callback=callback, throttle_seconds=None
        )
        trigger = threading.Thread(
            target=client._strategy_trigger_thread,
            name="StrategyTrigger",
        )
        client.threads.append(trigger)
        trigger.start()
        client.data_updated_event.set()
        assert callback_entered.wait(timeout=1)

        with patch.object(client, "_send_request", return_value=_request_failure()):
            stopper = threading.Thread(target=client.stop)
            stopper.start()
            time.sleep(0.05)
            assert stopper.is_alive()
            client.context.term.assert_not_called()

            release_callback.set()
            stopper.join(timeout=2)
            client.stop()

        assert not stopper.is_alive()
        assert not trigger.is_alive()
        client.context.term.assert_called_once_with()

    def test_concurrent_callback_stop_does_not_deadlock_cleanup_owner(self):
        callback_entered = threading.Event()
        allow_callback_stop = threading.Event()
        callback_stop_returned = threading.Event()

        def callback(_payload):
            callback_entered.set()
            assert allow_callback_stop.wait(timeout=2)
            client.stop()
            callback_stop_returned.set()

        client = _make_client(
            intervals=["1m"], callback=callback, throttle_seconds=None
        )
        trigger = threading.Thread(
            target=client._strategy_trigger_thread,
            name="StrategyTrigger",
        )
        client.threads.append(trigger)
        trigger.start()
        client.data_updated_event.set()
        assert callback_entered.wait(timeout=1)

        def unsubscribe(_request, **_kwargs):
            allow_callback_stop.set()
            return _request_failure()

        with patch.object(client, "_send_request", side_effect=unsubscribe):
            stopper = threading.Thread(target=client.stop)
            stopper.start()
            stopper.join(timeout=2)
            client.stop()

        assert callback_stop_returned.is_set()
        assert not stopper.is_alive()
        assert not trigger.is_alive()
        client.context.term.assert_called_once_with()

    def test_external_stop_waits_when_callback_starts_cleanup(self):
        callback_entered = threading.Event()
        unsubscribe_entered = threading.Event()
        release_unsubscribe = threading.Event()
        callback_stop_returned = threading.Event()
        release_callback = threading.Event()

        def callback(_payload):
            callback_entered.set()
            client.stop()
            callback_stop_returned.set()
            assert release_callback.wait(timeout=2)

        client = _make_client(
            intervals=["1m"], callback=callback, throttle_seconds=None
        )
        trigger = threading.Thread(
            target=client._strategy_trigger_thread,
            name="StrategyTrigger",
        )
        client.threads.append(trigger)

        def unsubscribe(_request, **_kwargs):
            unsubscribe_entered.set()
            assert release_unsubscribe.wait(timeout=2)
            return _request_failure()

        with patch.object(client, "_send_request", side_effect=unsubscribe):
            trigger.start()
            client.data_updated_event.set()
            assert callback_entered.wait(timeout=1)
            assert unsubscribe_entered.wait(timeout=1)

            external_stopper = threading.Thread(target=client.stop)
            external_stopper.start()
            time.sleep(0.05)
            assert external_stopper.is_alive()

            release_unsubscribe.set()
            assert callback_stop_returned.wait(timeout=1)
            time.sleep(0.05)
            assert external_stopper.is_alive()
            assert trigger.is_alive()

            release_callback.set()
            external_stopper.join(timeout=2)
            client.stop()

        assert not external_stopper.is_alive()
        assert not trigger.is_alive()
        client.context.term.assert_called_once_with()

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
        with patch.object(
            client, "_send_request", return_value=_request_failure()
        ) as send_request:
            client.stop()
            client.stop()  # second call — must not raise
            assert client.context.closed
        assert client.stop_event.is_set()
        assert client.context.closed
        assert send_request.call_count == 1

    def test_stop_completes_cleanup_when_unsubscribe_raises(self):
        client = _make_client(intervals=["1m"])

        with patch.object(client, "_send_request", side_effect=RuntimeError("failed")):
            client.stop()

        assert client.stop_event.is_set()
        client.context.term.assert_called_once_with()

    def test_stop_can_retry_after_context_termination_failure(self):
        client = _make_client(intervals=["1m"])
        client.context.term.side_effect = [RuntimeError("term failed"), None]

        with patch.object(client, "_send_request", return_value=_request_failure()):
            with pytest.raises(RuntimeError):
                client.stop()
            client.stop()

        assert client.context.term.call_count == 2

    def test_worker_stop_surfaces_cleanup_failure_and_allows_retry(self):
        cleanup_failed = threading.Event()

        def callback(_payload):
            try:
                client.stop()
            except RuntimeError:
                cleanup_failed.set()

        client = _make_client(
            intervals=["1m"], callback=callback, throttle_seconds=None
        )
        client.context.term.side_effect = [RuntimeError("term failed"), None]
        trigger = threading.Thread(
            target=client._strategy_trigger_thread,
            name="StrategyTrigger",
        )
        client.threads.append(trigger)

        with patch.object(client, "_send_request", return_value=_request_failure()):
            trigger.start()
            client.data_updated_event.set()
            assert cleanup_failed.wait(timeout=1)
            client.stop()

        assert not trigger.is_alive()
        assert client.context.term.call_count == 2


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


class TestLifecycleStartAndRenewal:
    def test_short_ttl_schedules_renewal_before_expiration(self):
        client = _make_client(server_candle_ttl=5)
        wait = _RecordingWait()

        with patch.object(type(client.stop_event), "wait", wait):
            client._subscription_renewer_thread()

        assert len(wait.timeouts) == 1
        assert 0 < wait.timeouts[0] < client.server_candle_ttl

    def test_send_request_interrupts_retry_backoff_on_shutdown(self):
        client = _make_client(intervals=["1m"])
        socket = MagicMock()
        socket.recv.side_effect = zmq.Again()
        client.context.socket.return_value = socket
        stop_event = _StopOnWait()
        client.stop_event = stop_event

        result = client._send_request({"action": "subscribe_candle"})

        assert result.code is GatewayRequestCode.CANCELED
        assert socket.send.call_count == 1
        assert stop_event.timeouts == [2]

    def test_renewer_does_not_count_failure_when_shutdown_starts_after_request(self):
        critical_calls = []
        client = _make_client(intervals=["1m"], on_critical=critical_calls.append)
        client.consecutive_renewal_failures = client.MAX_CONSECUTIVE_FAILURES - 1

        class StopBeforeFailureClassification:
            code = GatewayRequestCode.TIMEOUT

            @property
            def ok(self):
                client.stop_event.set()
                return False

        with (
            patch.object(
                client,
                "_send_request",
                return_value=StopBeforeFailureClassification(),
            ),
            patch.object(type(client.stop_event), "wait", _FiniteWait(1)),
        ):
            client._subscription_renewer_thread()

        assert client.consecutive_renewal_failures == 2
        assert critical_calls == []

    def test_successful_renewal_after_failures_resets_counter(self):
        responses = iter(
            [
                _request_failure(),
                _request_failure(),
                _request_ok(),
            ]
        )
        client = _make_client(intervals=["1m"])

        def pop_response(*_args, **_kwargs):
            return next(responses)

        with (
            patch.object(client, "_send_request", side_effect=pop_response),
            patch.object(type(client.stop_event), "wait", _FiniteWait(3)),
        ):
            client._subscription_renewer_thread()

        assert client.consecutive_renewal_failures == 0

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
            return _request_ok()

        with (
            patch.object(
                client.stop_event, "wait", side_effect=enter_one_renewal_cycle
            ),
            patch.object(client, "_send_request", side_effect=send_request),
        ):
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


@pytest.mark.parametrize("interval", ["10m", "1d", "1s", "2m"])
def test_reject_unsupported_aggregated_intervals(interval):
    with pytest.raises(ValueError):
        _make_client(intervals=[interval])


@pytest.mark.parametrize("mode", ["live", "deque_copy", "deep_copy"])
def test_callback_mutation_semantics(mode):
    client = _make_client(callback_snapshot_mode=mode)
    _inject_update(client, "1m", _make_candle(1))
    payload = client._build_callback_payload()
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
    client.data_updated_event.clear()
    client._handle_candle_event(
        "UPBIT:CANDLE:KRW-BTC:1m:UPDATE",
        dict(candle=_make_candle(1, 90), stream_id=STREAM, seq=1),
    )
    client._handle_candle_event(
        "UPBIT:CANDLE:KRW-BTC:1m:CLOSE",
        dict(
            candle=_make_candle(1, 90), new=_make_candle(2, 90), stream_id=STREAM, seq=2
        ),
    )
    _inject_reconcile(client, "1m", _make_candle(999))
    assert not client.data_updated_event.is_set()
    assert [c["ts"] for c in client.candle_deques["1m"]] == [1, 2, 3]
    assert client.candle_deques["1m"][0]["close"] == 101
    _inject_reconcile(client, "1m", _make_candle(1, 102))
    assert client.candle_deques["1m"][0]["close"] == 102
    assert client.data_updated_event.is_set()
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
    client._handle_candle_event(topic, dict(payload, stream_id=STREAM, seq=1))
    assert not client.candle_deques["1m"]
    assert not client.data_updated_event.is_set()


def test_snapshot_merges_live_close_without_duplicate_or_regression():
    client = _make_client()
    client._request_resync()

    def snapshot(request):
        _inject_update(client, "1m", _make_candle(2, 102))
        _inject_close(client, "1m", _make_candle(2, 103), _make_candle(3, 103))
        client._handle_candle_event(
            "UPBIT:CANDLE:KRW-BTC:1m:UPDATE",
            dict(candle=_make_candle(2, 99), stream_id=STREAM, seq=1),
        )
        return _request_ok([_make_candle(1), _make_candle(2)])

    with patch.object(client, "_send_request", side_effect=snapshot):
        assert client._load_snapshots()
    assert [c["ts"] for c in client.candle_deques["1m"]] == [1, 2, 3]
    assert [c["close"] for c in client.candle_deques["1m"]] == [100, 103, 103]


def test_failed_recovery_preserves_storage_and_retries_buffer():
    client = _make_client()
    _inject_update(client, "1m", _make_candle(1))
    client._handle_candle_event(
        "UPBIT:CANDLE:KRW-BTC:1m:RESYNC_REQUIRED", _event_payload(client, reason="gap")
    )
    _inject_update(client, "1m", _make_candle(3, 103))
    with patch.object(client, "_send_request", return_value=_request_failure()):
        assert not client._load_snapshots()
    assert [c["ts"] for c in client.candle_deques["1m"]] == [1]
    with patch.object(
        client,
        "_send_request",
        return_value=_request_ok([_make_candle(2)], snapshot_seq=2),
    ):
        assert client._load_snapshots()
    assert [c["ts"] for c in client.candle_deques["1m"]] == [2, 3]


def test_buffer_overflow_or_new_resync_invalidates_inflight_snapshot():
    client = _make_client()
    client._MAX_PENDING_EVENTS = 1
    client._request_resync()

    def snapshot(request):
        _inject_update(client, "1m", _make_candle(2))
        _inject_update(client, "1m", _make_candle(3))
        return _request_ok([_make_candle(1)])

    with patch.object(client, "_send_request", side_effect=snapshot):
        assert not client._load_snapshots()
    assert not client.candle_deques["1m"]
    with patch.object(
        client, "_send_request", return_value=_request_ok([_make_candle(2)])
    ):
        assert client._load_snapshots()
    assert [c["ts"] for c in client.candle_deques["1m"]] == [2, 3]


def test_trigger_releases_lock_and_survives_callback_error():
    calls = []
    done = threading.Event()

    def callback(payload):
        assert client.storage_lock.acquire(blocking=False)
        client.storage_lock.release()
        calls.append(payload["1m"][-1]["ts"])
        if len(calls) == 1:
            raise RuntimeError("user callback error")
        done.set()

    client = _make_client(callback=callback, throttle_seconds=0.01)
    worker = threading.Thread(target=client._strategy_trigger_thread)
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
        client.stop_event.set()
        worker.join(2)


@pytest.mark.parametrize(
    "failure", [_request_failure(), _request_ok([]), _request_ok([{}])]
)
def test_start_failure_leaves_no_threads_or_partial_data(failure):
    client = _make_client(intervals=["1m", "15m"])
    client.context.socket.return_value.get_monitor_socket.return_value.poll.return_value = False
    client.context.socket.return_value.poll.return_value = False
    with patch.object(
        client, "_send_request", side_effect=[_request_ok([_make_candle(1)]), failure]
    ):
        assert not client.start()
    assert not client.threads
    assert all(not candles for candles in client.candle_deques.values())
    client.context.socket.return_value.close.assert_called_once()


class _RecordingWait:
    def __init__(self):
        self.timeouts = []

    def __call__(self, timeout=None):
        self.timeouts.append(timeout)
        return True


@pytest.mark.parametrize("recovery", ["control", "reconnect", "restart"])
def test_real_socket_startup_and_recovery_handoff(recovery):
    """PUB events arriving before REP snapshots survive startup and recovery."""
    import orjson

    context = zmq.Context()
    ready = threading.Event()
    live_received = threading.Event()
    recovered = threading.Event()
    server_done = threading.Event()
    ports = []
    errors = []
    requests = []

    def server():
        rep = context.socket(zmq.REP)
        pub = context.socket(zmq.XPUB)
        rep.setsockopt(zmq.RCVTIMEO, 3000)
        pub.setsockopt(zmq.RCVTIMEO, 3000)
        ports.extend(
            [
                rep.bind_to_random_port("tcp://127.0.0.1"),
                pub.bind_to_random_port("tcp://127.0.0.1"),
            ]
        )
        ready.set()
        try:
            first = orjson.loads(rep.recv())
            requests.append(first)
            # XPUB confirms the listener subscribed before the snapshot is released.
            assert pub.recv().startswith(b"\x01UPBIT:CANDLE:KRW-BTC:15m:")
            pub.send_multipart([b"UPBIT:CANDLE:KRW-BTC:15m:UPDATE", b"invalid-json"])
            pub.send_multipart([b"UPBIT:CANDLE:KRW-BTC:15m:UPDATE"])
            pub.send_multipart(
                [
                    b"UPBIT:CANDLE:KRW-BTC:15m:CLOSE",
                    orjson.dumps(
                        {
                            "candle": _make_candle(2, 102),
                            "new": _make_candle(3, 103),
                            "stream_id": STREAM,
                            "seq": 10,
                        }
                    ),
                ]
            )
            assert live_received.wait(2)
            rep.send(
                orjson.dumps(
                    {
                        "status": "ok",
                        "data": [_make_candle(1), _make_candle(2)],
                        "stream_id": STREAM,
                        "snapshot_seq": 9,
                    }
                )
            )
            assert begin_recovery.wait(2)
            live_received.clear()
            if recovery == "control":
                pub.send_multipart(
                    [
                        b"UPBIT:CANDLE:KRW-BTC:15m:RESYNC_REQUIRED",
                        orjson.dumps(
                            {
                                "type": "RESYNC_REQUIRED",
                                "reason": "gap",
                                "stream_id": STREAM,
                                "seq": 11,
                            }
                        ),
                    ]
                )
            elif recovery == "restart":
                pub.send_multipart(
                    [
                        b"UPBIT:CANDLE:KRW-BTC:15m:UPDATE",
                        orjson.dumps(
                            {"candle": _make_candle(4), "stream_id": RESTART, "seq": 1}
                        ),
                    ]
                )
            else:
                pub.close(0)
                pub = context.socket(zmq.XPUB)
                pub.setsockopt(zmq.RCVTIMEO, 3000)
                pub.bind(f"tcp://127.0.0.1:{ports[1]}")
                assert pub.recv().startswith(b"\x01UPBIT:CANDLE:KRW-BTC:15m:")
            request = orjson.loads(rep.recv())
            requests.append(request)
            pub.send_multipart(
                [
                    b"UPBIT:CANDLE:KRW-BTC:15m:CLOSE",
                    orjson.dumps(
                        {
                            "candle": _make_candle(4, 104),
                            "new": _make_candle(5, 105),
                            "stream_id": RESTART if recovery == "restart" else STREAM,
                            "seq": 2 if recovery == "restart" else 13,
                        }
                    ),
                ]
            )
            assert live_received.wait(2)
            rep.send(
                orjson.dumps(
                    {
                        "status": "ok",
                        "data": [_make_candle(3, 130), _make_candle(4)],
                        "stream_id": RESTART if recovery == "restart" else STREAM,
                        "snapshot_seq": 1 if recovery == "restart" else 12,
                    }
                )
            )
            requests.append(orjson.loads(rep.recv()))
            rep.send(orjson.dumps({"status": "ok"}))
        except BaseException as exc:
            errors.append(exc)
        finally:
            rep.close(0)
            pub.close(0)
            server_done.set()

    begin_recovery = threading.Event()
    gateway = threading.Thread(target=server)
    gateway.start()
    assert ready.wait(2)

    def callback(payload):
        if payload["15m"] and payload["15m"][-1]["ts"] == 5:
            recovered.set()

    client = ZMQClient(
        client_id="integration",
        symbol="KRW-BTC",
        intervals=["15m"],
        candle_handler_callback=callback,
        throttle_seconds=0.01,
        zmq_gateway_host="127.0.0.1",
        zmq_gateway_req_port=ports[0],
        zmq_gateway_pub_port=ports[1],
    )
    original = client._handle_candle_event

    def receive(topic, payload):
        original(topic, payload)
        if topic.endswith(":CLOSE"):
            live_received.set()

    try:
        with patch.object(client, "_handle_candle_event", side_effect=receive):
            assert client.start()
            assert [c["ts"] for c in client._build_callback_payload()["15m"]] == [
                1,
                2,
                3,
            ]
            begin_recovery.set()
            assert recovered.wait(3)
            candles = list(client._build_callback_payload()["15m"])
            assert [c["ts"] for c in candles] == [3, 4, 5]
            assert [c["close"] for c in candles] == [130, 104, 105]
            client.stop()
            client.stop()
        assert server_done.wait(3)
        assert not errors
        assert [r["action"] for r in requests] == [
            "subscribe_candle",
            "subscribe_candle",
            "unsubscribe_candle",
        ]
        assert all(
            r["interval"] == "15m" and r["exchange"] == "upbit" for r in requests
        )
        assert all(not thread.is_alive() for thread in client.threads)
    finally:
        if not client.stop_event.is_set():
            with patch.object(client, "_send_request", return_value=_request_failure()):
                client.stop()
        gateway.join(4)
        context.term()


def test_sequence_merges_equal_volume_updates_and_discards_snapshot_covered_events():
    client = _make_client()
    client._request_resync()
    topic = "UPBIT:CANDLE:KRW-BTC:1m:UPDATE"
    for stream_id, seq, close in [
        (STREAM, 12, 112),
        (STREAM, 9, 99),
        (RESTART, 100, 999),
        (STREAM, 11, 111),
        (STREAM, 12, 90),
    ]:
        client._handle_candle_event(
            topic, dict(candle=_make_candle(2, close), stream_id=stream_id, seq=seq)
        )
    snapshot = _make_candle(2, 110)
    with patch.object(
        client, "_send_request", return_value=_request_ok([snapshot], snapshot_seq=10)
    ):
        assert client._load_snapshots()
    assert client.candle_deques["1m"][-1]["close"] == 112
    client.data_updated_event.clear()
    # Covered and duplicate events cannot mutate storage or trigger a callback.
    for seq in (10, 11, 12):
        client._handle_candle_event(
            topic, dict(candle=_make_candle(2, 90), stream_id=STREAM, seq=seq)
        )
    assert not client.data_updated_event.is_set()
    # A later sequence is authoritative even when volume decreases.
    latest = _make_candle(2, 113)
    latest["volume"] = 0
    client._handle_candle_event(topic, dict(candle=latest, stream_id=STREAM, seq=13))
    assert client.candle_deques["1m"][-1] == latest


def test_stale_snapshot_does_not_regress_existing_sequence():
    client = _make_client()
    _inject_update(client, "1m", _make_candle(3))
    client._request_resync()
    with patch.object(
        client,
        "_send_request",
        return_value=_request_ok([_make_candle(2)], snapshot_seq=0),
    ):
        assert not client._load_snapshots()
    assert [c["ts"] for c in client.candle_deques["1m"]] == [3]


def test_renewal_failures_trigger_critical_and_preserve_history():
    critical = []
    client = _make_client(on_critical=critical.append)
    _inject_update(client, "1m", _make_candle(1))
    with (
        patch.object(client, "_send_request", return_value=_request_failure()),
        patch.object(
            type(client.stop_event),
            "wait",
            _FiniteWait(client.MAX_CONSECUTIVE_FAILURES),
        ),
    ):
        client._subscription_renewer_thread()
    assert len(critical) == 1
    assert [c["ts"] for c in client.candle_deques["1m"]] == [1]


def test_stop_during_startup_cancels_publication_and_joins_listener():
    client = _make_client()
    socket = client.context.socket.return_value
    socket.get_monitor_socket.return_value.poll.return_value = False
    socket.poll.return_value = False
    request_entered = threading.Event()
    release_request = threading.Event()
    results = []
    actions = []

    def request(data, **kwargs):
        actions.append(data["action"])
        if data["action"] == "subscribe_candle":
            request_entered.set()
            assert release_request.wait(2)
        return _request_ok([_make_candle(1)])

    with patch.object(client, "_send_request", side_effect=request):
        starter = threading.Thread(target=lambda: results.append(client.start()))
        starter.start()
        assert request_entered.wait(2)
        stopper = threading.Thread(target=client.stop)
        stopper.start()
        assert client.stop_event.wait(1)
        release_request.set()
        starter.join(2)
        stopper.join(2)
    assert not starter.is_alive() and not stopper.is_alive()
    assert results == [False]
    assert not client.candle_deques["1m"]
    assert actions == ["subscribe_candle", "unsubscribe_candle"]
    socket.close.assert_called_once()
    client.context.term.assert_called_once()


@pytest.mark.parametrize(
    "worker", ["SubscriptionRenewer", "StrategyTrigger", "CandleRecovery"]
)
def test_worker_start_failure_rolls_back_without_orphan_threads(worker):
    client = _make_client()
    socket = client.context.socket.return_value
    socket.get_monitor_socket.return_value.poll.return_value = False
    socket.poll.return_value = False
    real_thread = threading.Thread
    created = []

    def thread_factory(*args, **kwargs):
        thread = real_thread(*args, **kwargs)
        created.append(thread)
        if thread.name == worker:
            thread.start = MagicMock(side_effect=RuntimeError("cannot start"))
        return thread

    with (
        patch.object(
            client, "_send_request", return_value=_request_ok([_make_candle(1)])
        ),
        patch("sk_zmq.client.threading.Thread", side_effect=thread_factory),
    ):
        assert not client.start()
        client.stop()
    assert all(not thread.is_alive() for thread in created)
    assert not client.candle_deques["1m"]
    client.context.term.assert_called_once()


def test_restart_resnapshots_all_intervals_without_comparing_runtime_counters():
    client = _make_client(intervals=["1m", "15m"])
    client._request_resync()
    with patch.object(
        client,
        "_send_request",
        return_value=_request_ok([_make_candle(1)], snapshot_seq=100),
    ):
        assert client._load_snapshots()
    # New runtime seq=1 must not be discarded against old runtime seq=100.
    client._handle_candle_event(
        "UPBIT:CANDLE:KRW-BTC:1m:UPDATE",
        dict(candle=_make_candle(2, 102), stream_id=RESTART, seq=1),
    )
    with patch.object(
        client,
        "_send_request",
        return_value=_request_ok([_make_candle(1)], snapshot_seq=101),
    ):
        assert not client._load_snapshots()
    assert all(c[-1]["ts"] == 1 for c in client.candle_deques.values())
    with patch.object(
        client,
        "_send_request",
        return_value=_request_ok(
            [_make_candle(2, 100)], stream_id=RESTART, snapshot_seq=0
        ),
    ) as request:
        assert client._load_snapshots()
    assert {call.args[0]["interval"] for call in request.call_args_list} == {
        "1m",
        "15m",
    }
    assert client.candle_deques["1m"][-1]["close"] == 102
    assert client.candle_deques["15m"][-1]["close"] == 100
    client.data_updated_event.clear()
    client._handle_candle_event(
        "UPBIT:CANDLE:KRW-BTC:1m:UPDATE",
        dict(candle=_make_candle(3, 999), stream_id=STREAM, seq=1000),
    )
    assert not client.data_updated_event.is_set()
    assert client.candle_deques["1m"][-1]["ts"] == 2
    client._handle_candle_event(
        "UPBIT:CANDLE:KRW-BTC:1m:UPDATE",
        dict(candle=_make_candle(2, 103), stream_id=RESTART, seq=2),
    )
    assert client.candle_deques["1m"][-1]["close"] == 103


def test_control_after_snapshot_boundary_requires_new_snapshot():
    client = _make_client()
    client._request_resync()
    client._handle_candle_event(
        "UPBIT:CANDLE:KRW-BTC:1m:RESYNC_REQUIRED", dict(stream_id=STREAM, seq=11)
    )
    with patch.object(
        client,
        "_send_request",
        return_value=_request_ok([_make_candle(1)], snapshot_seq=10),
    ):
        assert not client._load_snapshots()
    assert not client.candle_deques["1m"]
    with patch.object(
        client,
        "_send_request",
        return_value=_request_ok([_make_candle(2)], snapshot_seq=11),
    ):
        assert client._load_snapshots()
    client.data_updated_event.clear()
    # A duplicate control notice must not restart recovery.
    client._handle_candle_event(
        "UPBIT:CANDLE:KRW-BTC:1m:RESYNC_REQUIRED", dict(stream_id=STREAM, seq=11)
    )
    client._handle_candle_event(
        "UPBIT:CANDLE:KRW-BTC:1m:UPDATE",
        dict(candle=_make_candle(2, 102), stream_id=STREAM, seq=12),
    )
    assert client.candle_deques["1m"][-1]["close"] == 102


def test_restart_detected_by_renewal_keeps_data_until_full_snapshot():
    client = _make_client(intervals=["1m", "15m"])
    client._request_resync()
    with patch.object(
        client,
        "_send_request",
        return_value=_request_ok([_make_candle(1)], snapshot_seq=100),
    ):
        assert client._load_snapshots()
    with (
        patch.object(
            client,
            "_send_request",
            return_value=_request_ok(
                [_make_candle(2)], stream_id=RESTART, snapshot_seq=1
            ),
        ),
        patch.object(type(client.stop_event), "wait", _FiniteWait(1)),
    ):
        client._subscription_renewer_thread()
    assert all(c[-1]["ts"] == 1 for c in client.candle_deques.values())
    with patch.object(
        client,
        "_send_request",
        return_value=_request_ok([_make_candle(2)], stream_id=RESTART, snapshot_seq=1),
    ) as request:
        assert client._load_snapshots()
    assert request.call_count == 2
    assert all(c[-1]["ts"] == 2 for c in client.candle_deques.values())


def test_snapshot_restart_during_one_interval_recovery_expands_to_all():
    client = _make_client(intervals=["1m", "15m"])
    client._request_resync()
    with patch.object(
        client,
        "_send_request",
        return_value=_request_ok([_make_candle(1)], snapshot_seq=100),
    ):
        assert client._load_snapshots()
    client._handle_candle_event(
        "UPBIT:CANDLE:KRW-BTC:1m:RESYNC_REQUIRED", dict(stream_id=STREAM, seq=101)
    )
    with patch.object(
        client,
        "_send_request",
        return_value=_request_ok([_make_candle(2)], stream_id=RESTART, snapshot_seq=0),
    ) as request:
        assert not client._load_snapshots()
        assert client._load_snapshots()
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
    client._request_resync()
    result = GatewayRequestResult.success(
        data=[_make_candle(1)], response=dict(status="ok", **metadata)
    )
    with patch.object(client, "_send_request", return_value=result):
        assert not client._load_snapshots()
    assert not client.candle_deques["1m"]


def test_live_sequence_upserts_timestamp_without_replacing_newer_bucket():
    client = _make_client()
    _inject_close(client, "1m", _make_candle(1), _make_candle(2))
    _inject_update(client, "1m", _make_candle(1, 101))
    assert [c["ts"] for c in client.candle_deques["1m"]] == [1, 2]
    assert client.candle_deques["1m"][0]["close"] == 101


def test_restart_during_snapshot_request_invalidates_old_runtime_response():
    client = _make_client()
    client._request_resync()
    with patch.object(
        client,
        "_send_request",
        return_value=_request_ok([_make_candle(1)], snapshot_seq=100),
    ):
        assert client._load_snapshots()
    client._request_resync()

    def restarting_snapshot(request):
        client._handle_candle_event(
            "UPBIT:CANDLE:KRW-BTC:1m:UPDATE",
            dict(candle=_make_candle(2, 102), stream_id=RESTART, seq=1),
        )
        return _request_ok([_make_candle(3)], snapshot_seq=101)

    with patch.object(client, "_send_request", side_effect=restarting_snapshot):
        assert not client._load_snapshots()
    assert client.candle_deques["1m"][-1]["ts"] == 1
    with patch.object(
        client,
        "_send_request",
        return_value=_request_ok([_make_candle(2)], stream_id=RESTART, snapshot_seq=0),
    ):
        assert client._load_snapshots()
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
    client._handle_candle_event(
        "UPBIT:CANDLE:KRW-BTC:1m:UPDATE", dict(candle=_make_candle(1), **metadata)
    )
    assert not client.candle_deques["1m"]
    assert not client.data_updated_event.is_set()
