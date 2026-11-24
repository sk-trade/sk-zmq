"""Client startup, renewal, callback, and shutdown behavior."""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock, patch

import pytest
import zmq

from sk_zmq.client import GatewayRequestCode, ZMQClient

from ._helpers import (
    _COMMON_KWARGS,
    _FiniteWait,
    _inject_update,
    _make_candle,
    _make_client,
    _RecordingWait,
    _request_failure,
    _request_ok,
    _StopOnWait,
)


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
        callback_thread = threading.Thread(
            target=client._run_callback_loop,
            name="CandleCallback",
        )
        client._worker_threads.append(callback_thread)
        callback_thread.start()
        client._data_changed.set()
        assert callback_entered.wait(timeout=1)

        with patch.object(client, "_request_gateway", return_value=_request_failure()):
            stopper = threading.Thread(target=client.stop)
            stopper.start()
            time.sleep(0.05)
            assert stopper.is_alive()
            client._context.term.assert_not_called()

            release_callback.set()
            stopper.join(timeout=2)
            client.stop()

        assert not stopper.is_alive()
        assert not callback_thread.is_alive()
        client._context.term.assert_called_once_with()

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
        callback_thread = threading.Thread(
            target=client._run_callback_loop,
            name="CandleCallback",
        )
        client._worker_threads.append(callback_thread)
        callback_thread.start()
        client._data_changed.set()
        assert callback_entered.wait(timeout=1)

        def unsubscribe(_request, **_kwargs):
            allow_callback_stop.set()
            return _request_failure()

        with patch.object(client, "_request_gateway", side_effect=unsubscribe):
            stopper = threading.Thread(target=client.stop)
            stopper.start()
            stopper.join(timeout=2)
            client.stop()

        assert callback_stop_returned.is_set()
        assert not stopper.is_alive()
        assert not callback_thread.is_alive()
        client._context.term.assert_called_once_with()

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
        callback_thread = threading.Thread(
            target=client._run_callback_loop,
            name="CandleCallback",
        )
        client._worker_threads.append(callback_thread)

        def unsubscribe(_request, **_kwargs):
            unsubscribe_entered.set()
            assert release_unsubscribe.wait(timeout=2)
            return _request_failure()

        with patch.object(client, "_request_gateway", side_effect=unsubscribe):
            callback_thread.start()
            client._data_changed.set()
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
            assert callback_thread.is_alive()

            release_callback.set()
            external_stopper.join(timeout=2)
            client.stop()

        assert not external_stopper.is_alive()
        assert not callback_thread.is_alive()
        client._context.term.assert_called_once_with()

    def test_stop_is_idempotent_and_preserves_stop_request(self):
        """Calling stop() a second time must not raise and _stop_requested stays set.

        Use a real zmq.Context so the first stop can terminate it, then verify
        second stop does not re-enter network request flow or terminate again.
        """
        client = ZMQClient(
            intervals=["1m"],
            candle_handler_callback=lambda _d: None,
            **_COMMON_KWARGS,
        )
        with patch.object(
            client, "_request_gateway", return_value=_request_failure()
        ) as send_request:
            client.stop()
            client.stop()  # second call — must not raise
            assert client._context.closed
        assert client._stop_requested.is_set()
        assert client._context.closed
        assert send_request.call_count == 1

    def test_stop_completes_cleanup_when_unsubscribe_raises(self):
        client = _make_client(intervals=["1m"])

        with patch.object(
            client, "_request_gateway", side_effect=RuntimeError("failed")
        ):
            client.stop()

        assert client._stop_requested.is_set()
        client._context.term.assert_called_once_with()

    def test_stop_can_retry_after_context_termination_failure(self):
        client = _make_client(intervals=["1m"])
        client._context.term.side_effect = [RuntimeError("term failed"), None]

        with patch.object(client, "_request_gateway", return_value=_request_failure()):
            with pytest.raises(RuntimeError):
                client.stop()
            client.stop()

        assert client._context.term.call_count == 2

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
        client._context.term.side_effect = [RuntimeError("term failed"), None]
        callback_thread = threading.Thread(
            target=client._run_callback_loop,
            name="CandleCallback",
        )
        client._worker_threads.append(callback_thread)

        with patch.object(client, "_request_gateway", return_value=_request_failure()):
            callback_thread.start()
            client._data_changed.set()
            assert cleanup_failed.wait(timeout=1)
            client.stop()

        assert not callback_thread.is_alive()
        assert client._context.term.call_count == 2


class TestLifecycleStartAndRenewal:
    def test_short_ttl_schedules_renewal_before_expiration(self):
        client = _make_client(server_candle_ttl=5)
        wait = _RecordingWait()

        with patch.object(type(client._stop_requested), "wait", wait):
            client._run_renewal_loop()

        assert len(wait.timeouts) == 1
        assert 0 < wait.timeouts[0] < client.server_candle_ttl

    def test_request_gateway_interrupts_retry_backoff_on_shutdown(self):
        client = _make_client(intervals=["1m"])
        socket = MagicMock()
        socket.recv.side_effect = zmq.Again()
        client._context.socket.return_value = socket
        _stop_requested = _StopOnWait()
        client._stop_requested = _stop_requested

        result = client._request_gateway({"action": "subscribe_candle"})

        assert result.code is GatewayRequestCode.CANCELED
        assert socket.send.call_count == 1
        assert _stop_requested.timeouts == [2]

    def test_renewer_does_not_count_failure_when_shutdown_starts_after_request(self):
        critical_calls = []
        client = _make_client(intervals=["1m"], on_critical=critical_calls.append)
        client._renewal_failure_cycles = client._MAX_CONSECUTIVE_RENEWAL_FAILURES - 1

        class StopBeforeFailureClassification:
            code = GatewayRequestCode.TIMEOUT

            @property
            def ok(self):
                client._stop_requested.set()
                return False

        with (
            patch.object(
                client,
                "_request_gateway",
                return_value=StopBeforeFailureClassification(),
            ),
            patch.object(type(client._stop_requested), "wait", _FiniteWait(1)),
        ):
            client._run_renewal_loop()

        assert client._renewal_failure_cycles == 2
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
            patch.object(client, "_request_gateway", side_effect=pop_response),
            patch.object(type(client._stop_requested), "wait", _FiniteWait(3)),
        ):
            client._run_renewal_loop()

        assert client._renewal_failure_cycles == 0

    def test_stop_unsubscribes_after_in_flight_renewal(self):
        critical_calls = []
        client = _make_client(
            intervals=["1m", "5m"],
            on_critical=critical_calls.append,
        )
        real_wait = client._stop_requested.wait
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
                client._stop_requested, "wait", side_effect=enter_one_renewal_cycle
            ),
            patch.object(client, "_request_gateway", side_effect=send_request),
        ):
            renewer = threading.Thread(target=client._run_renewal_loop)
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
        assert client._renewal_failure_cycles == 0
        assert critical_calls == []


@pytest.mark.parametrize(
    "failure", [_request_failure(), _request_ok([]), _request_ok([{}])]
)
def test_start_failure_leaves_no_threads_or_partial_data(failure):
    client = _make_client(intervals=["1m", "15m"])
    client._context.socket.return_value.get_monitor_socket.return_value.poll.return_value = False
    client._context.socket.return_value.poll.return_value = False
    with patch.object(
        client,
        "_request_gateway",
        side_effect=[_request_ok([_make_candle(1)]), failure],
    ):
        assert not client.start()
    assert not client._worker_threads
    assert all(not candles for candles in client.candle_deques.values())
    client._context.socket.return_value.close.assert_called_once()


def test_renewal_failures_trigger_critical_and_preserve_history():
    critical = []
    client = _make_client(on_critical=critical.append)
    _inject_update(client, "1m", _make_candle(1))
    with (
        patch.object(client, "_request_gateway", return_value=_request_failure()),
        patch.object(
            type(client._stop_requested),
            "wait",
            _FiniteWait(client._MAX_CONSECUTIVE_RENEWAL_FAILURES),
        ),
    ):
        client._run_renewal_loop()
    assert len(critical) == 1
    assert [c["ts"] for c in client.candle_deques["1m"]] == [1]


def test_stop_during_startup_cancels_publication_and_joins_listener():
    client = _make_client()
    socket = client._context.socket.return_value
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

    with patch.object(client, "_request_gateway", side_effect=request):
        starter = threading.Thread(target=lambda: results.append(client.start()))
        starter.start()
        assert request_entered.wait(2)
        stopper = threading.Thread(target=client.stop)
        stopper.start()
        assert client._stop_requested.wait(1)
        release_request.set()
        starter.join(2)
        stopper.join(2)
    assert not starter.is_alive() and not stopper.is_alive()
    assert results == [False]
    assert not client.candle_deques["1m"]
    assert actions == ["subscribe_candle", "unsubscribe_candle"]
    socket.close.assert_called_once()
    client._context.term.assert_called_once()


@pytest.mark.parametrize(
    "worker", ["SubscriptionRenewer", "CandleCallback", "CandleRecovery"]
)
def test_worker_start_failure_rolls_back_without_orphan_threads(worker):
    client = _make_client()
    socket = client._context.socket.return_value
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
            client, "_request_gateway", return_value=_request_ok([_make_candle(1)])
        ),
        patch("sk_zmq.client.threading.Thread", side_effect=thread_factory),
    ):
        assert not client.start()
        client.stop()
    assert all(not thread.is_alive() for thread in created)
    assert not client.candle_deques["1m"]
    client._context.term.assert_called_once()
