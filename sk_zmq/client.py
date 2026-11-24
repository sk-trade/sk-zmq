"""Threaded ZeroMQ client for ordered candle streams."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping, Sequence
from enum import Enum
from typing import Any, Final, TypeAlias

import orjson
import zmq
from zmq.utils.monitor import recv_monitor_message

from ._config import ClientConfig, SnapshotMode
from ._gateway import send_gateway_request
from ._protocol import (
    GatewayRequestCode,
    GatewayRequestResult,
    describe_request_failure,
    extract_snapshot,
    has_valid_ordering_metadata,
    parse_candle_event,
)
from ._store import (
    CandleDequeMap,
    CandleStore,
    RecoveryCommit,
    RecoveryPlan,
    RecoverySnapshot,
)

logger = logging.getLogger(__name__)

CandleCallback: TypeAlias = Callable[[CandleDequeMap], None]
CriticalCallback: TypeAlias = Callable[[str], None]


class ClientState(str, Enum):
    """Lifecycle states exposed through :attr:`ZMQClient.state`."""

    CREATED = "created"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOP_FAILED = "stop_failed"
    STOPPED = "stopped"


class ZMQClient:
    """Receive, recover, and expose an ordered candle stream from a ZMQ gateway.

    The client owns one REQ request path and one SUB listener. Incoming events are
    ordered by ``stream_id`` and ``seq``. Snapshot recovery is transactional: live
    events are buffered until every requested interval has a compatible snapshot.

    ``start()`` and ``stop()`` are safe to call from different threads. The candle
    callback always runs without the internal data lock held.
    """

    _UNSUBSCRIBE_TIMEOUT_MS: Final = 1_000
    _MAX_INITIAL_HISTORY_COUNT: Final = 200
    _MAX_CONSECUTIVE_RENEWAL_FAILURES: Final = 3
    _MAX_BUFFERED_EVENTS_PER_INTERVAL: Final = 10_000
    _LISTENER_READY_TIMEOUT_SECONDS: Final = 5.0
    _THREAD_JOIN_TIMEOUT_SECONDS: Final = 5.0
    _SUBSCRIBER_POLL_TIMEOUT_MS: Final = 10
    _CANDLE_FRAME_COUNT: Final = 2

    def __init__(
        self,
        client_id: str,
        symbol: str,
        intervals: Sequence[str],
        candle_handler_callback: CandleCallback,
        throttle_seconds: float | None = 0.1,
        *,
        zmq_gateway_host: str = "localhost",
        zmq_gateway_req_port: int = 11556,
        zmq_gateway_pub_port: int = 11558,
        server_candle_ttl: float = 300,
        candle_deque_maxlen: int = 200,
        on_critical: CriticalCallback | None = None,
        callback_snapshot_mode: SnapshotMode = "deque_copy",
        exchange: str = "upbit",
    ) -> None:
        config = ClientConfig.create(
            client_id=client_id,
            symbol=symbol,
            intervals=intervals,
            exchange=exchange,
            gateway_host=zmq_gateway_host,
            gateway_request_port=zmq_gateway_req_port,
            gateway_publish_port=zmq_gateway_pub_port,
            subscription_ttl_seconds=server_candle_ttl,
            candle_capacity=candle_deque_maxlen,
            callback_throttle_seconds=throttle_seconds,
            callback_snapshot_mode=callback_snapshot_mode,
        )
        if not callable(candle_handler_callback):
            raise TypeError("candle_handler_callback must be callable")
        if on_critical is not None and not callable(on_critical):
            raise TypeError("on_critical must be callable")

        self.client_id = config.client_id
        self.symbol = config.symbol
        self.intervals = list(config.intervals)
        self.exchange = config.exchange
        self.exchange_prefix = config.exchange_prefix
        self.candle_handler_callback = candle_handler_callback
        self.throttle_seconds = config.callback_throttle_seconds
        self.zmq_gateway_host = config.gateway_host
        self.zmq_gateway_req_port = config.gateway_request_port
        self.zmq_gateway_pub_port = config.gateway_publish_port
        self.server_candle_ttl = config.subscription_ttl_seconds
        self.candle_deque_maxlen = config.candle_capacity
        self.on_critical = on_critical
        self.callback_snapshot_mode: SnapshotMode = config.callback_snapshot_mode

        self._stop_requested = threading.Event()
        self._data_changed = threading.Event()
        self._transport_connected = threading.Event()
        self._recovery_requested = threading.Event()
        self._listener_stop_requested = threading.Event()
        self._startup_finished = threading.Event()
        self._startup_finished.set()
        self._startup_aborted = threading.Event()
        self._shutdown_finished = threading.Event()

        self._store = CandleStore(self.intervals, self.candle_deque_maxlen)
        self._lifecycle_lock = threading.Lock()
        self._request_lock = threading.Lock()
        self._state = ClientState.CREATED
        self._renewal_failure_cycles = 0
        self._worker_threads: list[threading.Thread] = []

        # Allocate external resources only after every pure input check succeeds.
        self._context = zmq.Context()

    @property
    def state(self) -> ClientState:
        """Return the current lifecycle state."""
        with self._lifecycle_lock:
            return self._state

    @property
    def is_running(self) -> bool:
        """Return whether the client is running and all its workers are alive."""
        with self._lifecycle_lock:
            return (
                self._state is ClientState.RUNNING
                and bool(self._worker_threads)
                and all(worker.is_alive() for worker in self._worker_threads)
            )

    @property
    def candle_deques(self) -> CandleDequeMap:
        """Return the live internal candle map for backward compatibility.

        Prefer :meth:`get_candle_snapshot` unless the caller deliberately accepts
        concurrent mutation.
        """
        return self._store.live_candles

    def get_candle_snapshot(self, mode: SnapshotMode = "deep_copy") -> CandleDequeMap:
        """Return candle data using ``live``, ``deque_copy``, or ``deep_copy``."""
        return self._store.snapshot(mode)

    def _build_callback_snapshot(self) -> CandleDequeMap:
        return self._store.snapshot(self.callback_snapshot_mode)

    def _build_subscription_request(
        self,
        action: str,
        interval: str,
        *,
        history_count: int | None = None,
    ) -> dict[str, Any]:
        request: dict[str, Any] = {
            "action": action,
            "symbol": self.symbol,
            "interval": interval,
            "exchange": self.exchange,
        }
        if history_count is not None:
            request["history_count"] = history_count
        return request

    def _request_gateway(
        self,
        request: Mapping[str, Any],
        *,
        max_attempts: int = 5,
        initial_backoff_seconds: float = 2,
        response_timeout_ms: int = 10_000,
    ) -> GatewayRequestResult:
        return send_gateway_request(
            context=self._context,
            stop_requested=self._stop_requested,
            host=self.zmq_gateway_host,
            port=self.zmq_gateway_req_port,
            request=request,
            max_attempts=max_attempts,
            initial_backoff_seconds=initial_backoff_seconds,
            response_timeout_ms=response_timeout_ms,
        )

    def _mark_recovery_required(
        self,
        intervals: Sequence[str] | None = None,
        *,
        stream_id: str | None = None,
    ) -> None:
        self._store.request_recovery(intervals, stream_id=stream_id)
        self._recovery_requested.set()

    def _ingest_publication(self, topic: str, payload: object) -> None:
        event = parse_candle_event(
            topic,
            payload,
            expected_exchange_prefix=self.exchange_prefix,
            expected_symbol=self.symbol,
            subscribed_intervals=self._store.live_candles,
        )
        if event is None:
            return

        update = self._store.ingest(
            event,
            max_buffered_events=self._MAX_BUFFERED_EVENTS_PER_INTERVAL,
        )
        if update.recovery_requested:
            self._recovery_requested.set()
        if update.candles_changed:
            self._data_changed.set()

    def _fetch_recovery_snapshot(
        self,
        interval: str,
    ) -> RecoverySnapshot | None:
        with self._request_lock:
            if self._stop_requested.is_set():
                return None
            result = self._request_gateway(
                self._build_subscription_request(
                    "subscribe_candle",
                    interval,
                    history_count=min(
                        self.candle_deque_maxlen,
                        self._MAX_INITIAL_HISTORY_COUNT,
                    ),
                )
            )

        candles = extract_snapshot(result)
        response = result.response
        if candles is None or not has_valid_ordering_metadata(
            response,
            sequence_key="snapshot_seq",
        ):
            return None
        if response is None:
            return None
        return RecoverySnapshot(
            candles=candles,
            stream_id=response["stream_id"],
            sequence=response["snapshot_seq"],
        )

    def _finish_recovery(
        self,
        plan: RecoveryPlan,
        snapshots: Mapping[str, RecoverySnapshot],
    ) -> bool:
        if self._stop_requested.is_set():
            return False

        commit: RecoveryCommit = self._store.commit_recovery(plan, snapshots)
        if commit.expanded_to_all_intervals:
            self._recovery_requested.set()
        if not commit.accepted:
            return False

        self._refresh_recovery_signal()
        if commit.candles_changed:
            self._data_changed.set()
        return True

    def _refresh_recovery_signal(self) -> None:
        # A new recovery may arrive immediately after a snapshot commit.
        self._recovery_requested.clear()
        if self._store.begin_recovery().intervals:
            self._recovery_requested.set()

    def _recover_pending_intervals(self) -> bool:
        plan = self._store.begin_recovery()
        if not plan.intervals:
            self._refresh_recovery_signal()
            return True

        snapshots: dict[str, RecoverySnapshot] = {}
        for interval in plan.intervals:
            snapshot = self._fetch_recovery_snapshot(interval)
            if snapshot is None:
                return False
            snapshots[interval] = snapshot
        return self._finish_recovery(plan, snapshots)

    def _notify_critical(self, message: str) -> None:
        if self.on_critical is None:
            return
        try:
            self.on_critical(message)
        except Exception:
            logger.exception("Critical-event callback failed")

    def _run_recovery_loop(self) -> None:
        while not self._stop_requested.is_set():
            if not self._transport_connected.wait(0.1):
                continue
            if not self._recovery_requested.wait(0.1):
                continue
            if self._recover_pending_intervals():
                continue

            logger.error(
                "Candle recovery failed; live events remain buffered until retry"
            )
            self._notify_critical(
                "Candle recovery failed; callback delivery is paused until a valid snapshot is loaded."
            )
            self._stop_requested.wait(1)

    @staticmethod
    def _close_socket(socket: Any | None, *, label: str) -> None:
        if socket is None:
            return
        try:
            socket.close(linger=0)
        except Exception:
            logger.exception("Could not close %s socket", label)

    def _open_event_stream(self) -> tuple[Any, Any]:
        subscriber = self._context.socket(zmq.SUB)
        monitor = None
        try:
            monitor = subscriber.get_monitor_socket(
                events=zmq.EVENT_CONNECTED | zmq.EVENT_DISCONNECTED
            )
            subscriber.connect(
                f"tcp://{self.zmq_gateway_host}:{self.zmq_gateway_pub_port}"
            )
            for interval in self.intervals:
                topic = f"{self.exchange_prefix}:CANDLE:{self.symbol}:{interval}:"
                subscriber.setsockopt_string(zmq.SUBSCRIBE, topic)
                logger.debug("Client %s subscribed to %s", self.client_id, topic)
        except Exception:
            self._close_socket(monitor, label="ZMQ monitor")
            self._close_socket(subscriber, label="ZMQ subscriber")
            raise
        return subscriber, monitor

    def _process_connection_events(self, monitor: Any) -> None:
        while monitor.poll(0):
            connection_event = recv_monitor_message(monitor).get("event")
            if connection_event == zmq.EVENT_DISCONNECTED:
                self._transport_connected.clear()
                self._mark_recovery_required()
            elif connection_event == zmq.EVENT_CONNECTED:
                self._transport_connected.set()

    def _receive_candle_publication(
        self,
        subscriber: Any,
    ) -> tuple[str, object] | None:
        try:
            if not subscriber.poll(self._SUBSCRIBER_POLL_TIMEOUT_MS):
                return None
            frames = subscriber.recv_multipart()
            if (
                not isinstance(frames, (list, tuple))
                or len(frames) != self._CANDLE_FRAME_COUNT
            ):
                logger.warning("Ignoring malformed candle frames: %r", frames)
                return None

            topic_bytes, payload_bytes = frames
            if not isinstance(topic_bytes, bytes) or not isinstance(
                payload_bytes, bytes
            ):
                logger.warning("Ignoring non-bytes candle frames: %r", frames)
                return None
            return topic_bytes.decode(), orjson.loads(payload_bytes)
        except zmq.Again:
            return None
        except (orjson.JSONDecodeError, UnicodeDecodeError, TypeError) as error:
            logger.warning("Ignoring malformed candle publication: %s", error)
            return None

    def _run_event_listener(
        self,
        ready_event: threading.Event | None = None,
        failed_event: threading.Event | None = None,
    ) -> None:
        subscriber = None
        monitor = None
        try:
            try:
                subscriber, monitor = self._open_event_stream()
            except Exception:
                logger.exception("Could not initialize ZMQ subscriber")
                if failed_event is not None:
                    failed_event.set()
                return
            finally:
                if ready_event is not None:
                    ready_event.set()

            while (
                not self._stop_requested.is_set()
                and not self._listener_stop_requested.is_set()
            ):
                self._process_connection_events(monitor)
                publication = self._receive_candle_publication(subscriber)
                if publication is not None:
                    self._ingest_publication(*publication)
        except Exception:
            logger.exception("ZMQ event listener stopped after an unexpected error")
            self._notify_critical(
                "The ZMQ event listener stopped unexpectedly; restart the client."
            )
        finally:
            self._close_socket(monitor, label="ZMQ monitor")
            self._close_socket(subscriber, label="ZMQ subscriber")
            logger.debug("Client %s event listener stopped", self.client_id)

    def _renew_interval(self, interval: str) -> GatewayRequestResult | None:
        request = self._build_subscription_request(
            "subscribe_candle",
            interval,
            history_count=1,
        )
        with self._request_lock:
            if self._stop_requested.is_set():
                return None
            result = self._request_gateway(request)

        if self._stop_requested.is_set():
            return None
        if not result.ok:
            return result
        if not has_valid_ordering_metadata(
            result.response,
            sequence_key="snapshot_seq",
        ):
            return GatewayRequestResult.failure(GatewayRequestCode.INVALID_RESPONSE)

        response = result.response
        if response is None:
            return GatewayRequestResult.failure(GatewayRequestCode.INVALID_RESPONSE)
        if self._store.observe_runtime(interval, response["stream_id"]):
            self._recovery_requested.set()
        return result

    def _record_renewal_cycle(
        self,
        failed_intervals: Sequence[tuple[str, str]],
    ) -> None:
        if not failed_intervals:
            if self._renewal_failure_cycles:
                logger.info(
                    "Subscription renewal recovered after %d failed cycles",
                    self._renewal_failure_cycles,
                )
            self._renewal_failure_cycles = 0
            return

        self._renewal_failure_cycles += 1
        logger.warning(
            "Subscription renewal cycle failed for %d interval(s); consecutive cycles=%d",
            len(failed_intervals),
            self._renewal_failure_cycles,
        )
        if self._renewal_failure_cycles != self._MAX_CONSECUTIVE_RENEWAL_FAILURES:
            return

        failure_summary = ", ".join(
            f"{interval}={detail}" for interval, detail in failed_intervals
        )
        self._notify_critical(
            "ZMQ subscription renewal failed for "
            f"{self._renewal_failure_cycles} consecutive cycles "
            f"(client_id={self.client_id}; {failure_summary})."
        )

    def _run_renewal_loop(self) -> None:
        renewal_period_seconds = self.server_candle_ttl / 2

        while not self._stop_requested.wait(renewal_period_seconds):
            failed_intervals: list[tuple[str, str]] = []
            for interval in self.intervals:
                result = self._renew_interval(interval)
                if result is None:
                    break
                if result.ok:
                    continue

                failure_detail = describe_request_failure(result)
                failed_intervals.append((interval, failure_detail))
                logger.error(
                    "Subscription renewal failed for %s: %s",
                    interval,
                    failure_detail,
                )

            if self._stop_requested.is_set():
                break
            self._record_renewal_cycle(failed_intervals)

        logger.debug("Client %s renewal loop stopped", self.client_id)

    def _run_callback_loop(self) -> None:
        callback_period_seconds = (
            self.throttle_seconds
            if self.throttle_seconds is not None and self.throttle_seconds > 0
            else None
        )

        while not self._stop_requested.is_set():
            if callback_period_seconds is None:
                data_changed = self._data_changed.wait(timeout=1)
            else:
                if self._stop_requested.wait(callback_period_seconds):
                    break
                data_changed = self._data_changed.is_set()

            if self._stop_requested.is_set():
                break
            if not data_changed:
                continue

            self._data_changed.clear()
            callback_snapshot = self._store.snapshot_if_ready(
                self.callback_snapshot_mode
            )
            if callback_snapshot is None:
                self._data_changed.set()
                self._stop_requested.wait(0.1)
                continue

            try:
                self.candle_handler_callback(callback_snapshot)
            except Exception:
                logger.exception("Candle callback failed")

        logger.debug("Client %s callback loop stopped", self.client_id)

    def _run_after_startup(self, worker: Callable[[], None]) -> None:
        self._startup_finished.wait()
        if not self._stop_requested.is_set() and not self._startup_aborted.is_set():
            worker()

    def _clear_runtime_state(self) -> None:
        self._store.clear()
        self._recovery_requested.clear()
        self._transport_connected.clear()
        self._data_changed.clear()

    def _abort_start(self) -> bool:
        self._clear_runtime_state()
        with self._lifecycle_lock:
            if self._state is ClientState.STARTING:
                self._state = ClientState.CREATED
            self._startup_finished.set()
        return False

    def _stop_prepared_threads(
        self,
        prepared_threads: Sequence[threading.Thread],
    ) -> None:
        self._startup_aborted.set()
        self._listener_stop_requested.set()
        self._startup_finished.set()
        current_thread = threading.current_thread()
        for thread in prepared_threads:
            if thread is current_thread or not thread.is_alive():
                continue
            thread.join(timeout=self._THREAD_JOIN_TIMEOUT_SECONDS)
            if thread.is_alive():
                logger.error(
                    "Thread %s did not stop during startup rollback", thread.name
                )

    def _begin_start(self) -> bool:
        with self._lifecycle_lock:
            if self._state is not ClientState.CREATED or self._stop_requested.is_set():
                return False
            self._state = ClientState.STARTING
            self._startup_finished.clear()
            self._startup_aborted.clear()
        return True

    def _start_listener_thread(self) -> threading.Thread | None:
        listener_ready = threading.Event()
        listener_failed = threading.Event()
        listener = threading.Thread(
            target=self._run_event_listener,
            args=(listener_ready, listener_failed),
            name="ZMQListener",
        )
        try:
            listener.start()
        except Exception:
            logger.exception("Could not start ZMQ listener thread")
            return None

        if not listener_ready.wait(timeout=self._LISTENER_READY_TIMEOUT_SECONDS):
            logger.error("ZMQ listener did not report readiness before timeout")
            self._stop_prepared_threads([listener])
            return None
        if listener_failed.is_set():
            self._stop_prepared_threads([listener])
            return None
        return listener

    def _start_worker_threads(
        self,
        prepared_threads: list[threading.Thread],
    ) -> bool:
        worker_specs = (
            ("SubscriptionRenewer", self._run_renewal_loop),
            ("CandleCallback", self._run_callback_loop),
            ("CandleRecovery", self._run_recovery_loop),
        )
        try:
            for thread_name, worker in worker_specs:
                if self._stop_requested.is_set():
                    return False
                thread = threading.Thread(
                    target=self._run_after_startup,
                    args=(worker,),
                    name=thread_name,
                )
                thread.start()
                prepared_threads.append(thread)
        except Exception:
            logger.exception("Could not start ZMQ worker thread")
            return False
        return True

    def _publish_running_state(
        self,
        prepared_threads: list[threading.Thread],
    ) -> bool:
        with self._store.lock, self._lifecycle_lock:
            startup_canceled = (
                self._state is not ClientState.STARTING or self._stop_requested.is_set()
            )
            if startup_canceled:
                return False
            self._worker_threads = prepared_threads
            self._state = ClientState.RUNNING
            self._startup_finished.set()
        return True

    def _rollback_start(
        self,
        prepared_threads: Sequence[threading.Thread],
    ) -> bool:
        self._stop_prepared_threads(prepared_threads)
        return self._abort_start()

    def start(self) -> bool:
        """Start the listener, install snapshots, and publish worker threads."""
        if not self._begin_start():
            return False

        logger.info("Starting ZMQ client %s", self.client_id)
        self._listener_stop_requested.clear()
        self._mark_recovery_required()

        listener = self._start_listener_thread()
        if listener is None:
            return self._abort_start()

        prepared_threads = [listener]
        if not self._recover_pending_intervals():
            return self._rollback_start(prepared_threads)
        if not self._start_worker_threads(prepared_threads):
            return self._rollback_start(prepared_threads)
        if not self._publish_running_state(prepared_threads):
            return self._rollback_start(prepared_threads)

        logger.info("ZMQ client %s is running", self.client_id)
        return True

    def _mark_stopped(self) -> None:
        with self._lifecycle_lock:
            self._state = ClientState.STOPPED
            self._shutdown_finished.set()
        logger.info("ZMQ client %s stopped", self.client_id)

    def _mark_stopped_after_thread(self, thread: threading.Thread) -> None:
        try:
            thread.join()
        except BaseException:
            with self._lifecycle_lock:
                self._state = ClientState.STOP_FAILED
                self._shutdown_finished.set()
            logger.exception("Could not wait for shutdown owner thread")
            return
        self._mark_stopped()

    def _perform_shutdown(
        self,
        completion_thread: threading.Thread | None = None,
    ) -> None:
        logger.info("Stopping ZMQ client %s", self.client_id)
        try:
            with self._request_lock:
                for interval in self.intervals:
                    try:
                        self._request_gateway(
                            self._build_subscription_request(
                                "unsubscribe_candle",
                                interval,
                            ),
                            max_attempts=1,
                            response_timeout_ms=self._UNSUBSCRIBE_TIMEOUT_MS,
                        )
                    except Exception:
                        logger.exception(
                            "Unsubscribe request failed for interval %s",
                            interval,
                        )

            current_thread = threading.current_thread()
            for thread in self._worker_threads:
                if thread is not current_thread and thread.is_alive():
                    thread.join()

            self._context.term()

            if completion_thread is not None:
                finalizer = threading.Thread(
                    target=self._mark_stopped_after_thread,
                    args=(completion_thread,),
                    name="ZMQStopFinalizer",
                    daemon=False,
                )
                finalizer.start()
                return
        except BaseException:
            with self._lifecycle_lock:
                self._state = ClientState.STOP_FAILED
                self._shutdown_finished.set()
            raise

        self._mark_stopped()

    def stop(self) -> None:
        """Stop workers, release subscriptions, and terminate the ZMQ context."""
        caller = threading.current_thread()
        startup_finished: threading.Event | None = None

        while True:
            with self._lifecycle_lock:
                if self._state is ClientState.STOPPED:
                    return
                if self._state is ClientState.STOPPING:
                    if caller in self._worker_threads:
                        return
                    shutdown_finished = self._shutdown_finished
                else:
                    if self._state is ClientState.STARTING:
                        startup_finished = self._startup_finished
                    self._state = ClientState.STOPPING
                    self._shutdown_finished.clear()
                    self._stop_requested.set()
                    break

            shutdown_finished.wait()

        if startup_finished is not None:
            startup_finished.wait()

        completion_thread = caller if caller in self._worker_threads else None
        self._perform_shutdown(completion_thread)

    def __enter__(self) -> ZMQClient:
        if not self.start():
            self.stop()
            raise RuntimeError("ZMQ client could not start")
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.stop()
