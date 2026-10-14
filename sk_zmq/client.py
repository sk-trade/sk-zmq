import logging
import threading

from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Mapping, Optional

import orjson
import zmq
from zmq.utils.monitor import recv_monitor_message

logger = logging.getLogger(__name__)


class GatewayRequestCode(Enum):
    OK = "ok"
    CANCELED = "canceled"
    TIMEOUT = "timeout"
    INVALID_REQUEST = "invalid_request"
    TRANSPORT_ERROR = "transport_error"
    INVALID_RESPONSE = "invalid_response"
    GATEWAY_REJECTED = "gateway_rejected"


@dataclass(frozen=True)
class GatewayRequestResult:
    code: GatewayRequestCode
    data: Any = None
    response: Optional[Mapping[str, Any]] = None

    @property
    def ok(self) -> bool:
        return self.code is GatewayRequestCode.OK

    @classmethod
    def success(
        cls, data: Any = None, response: Optional[Mapping[str, Any]] = None
    ) -> "GatewayRequestResult":
        return cls(GatewayRequestCode.OK, data=data, response=response)

    @classmethod
    def failure(
        cls,
        code: GatewayRequestCode,
        response: Optional[Mapping[str, Any]] = None,
    ) -> "GatewayRequestResult":
        if code is GatewayRequestCode.OK:
            raise ValueError("failure result cannot use GatewayRequestCode.OK")
        return cls(code, response=response)


class CandleEventType(Enum):
    UPDATE = "UPDATE"
    CLOSE = "CLOSE"
    RECONCILE = "RECONCILE"
    RESYNC_REQUIRED = "RESYNC_REQUIRED"


@dataclass(frozen=True)
class CandleEvent:
    interval: str
    event_type: CandleEventType
    stream_id: str
    seq: int
    candle: Optional[Dict[str, Any]] = None
    new_candle: Optional[Dict[str, Any]] = None


class ClientState(Enum):
    CREATED = "created"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOP_FAILED = "stop_failed"
    STOPPED = "stopped"


class ZMQClient:
    """
    ZMQ 게이트웨이와 통신하여 실시간 캔들 데이터를 수신하고 관리하는 클라이언트.

    이 클래스는 다음을 담당합니다:
    1. ZMQ REQ-REP 패턴을 사용해 게이트웨이에 초기 데이터(스냅샷) 및 구독 요청.
    2. ZMQ PUB-SUB 패턴을 사용해 실시간으로 브로드캐스트되는 캔들 데이터 수신.
    3. 수신된 캔들 데이터를 내부 deque에 스레드에 안전하게 저장 및 관리.
    4. 주기적으로 구독을 갱신하여 연결을 유지.
    5. 데이터 업데이트 시 등록된 콜백 함수(전략)를 트리거.
    6. 별도의 스레드에서 모든 네트워크 I/O 및 주기적 작업을 처리하여 메인 스레드를 블로킹하지 않음.
    """

    _STOP_UNSUBSCRIBE_TIMEOUT_MS = 1000
    _MAX_INITIAL_HISTORY_COUNT = 200
    MAX_CONSECUTIVE_FAILURES = 3
    _MAX_PENDING_EVENTS = 10000

    def __init__(
        self,
        client_id: str,
        symbol: str,
        intervals: List[str],
        candle_handler_callback: Callable[[Dict[str, deque]], None],
        throttle_seconds: Optional[float] = 0.1,
        *,
        zmq_gateway_host: str = "localhost",
        zmq_gateway_req_port: int = 11556,
        zmq_gateway_pub_port: int = 11558,
        server_candle_ttl: int = 300,
        candle_deque_maxlen: int = 200,
        on_critical: Optional[Callable[[str], None]] = None,
        callback_snapshot_mode: str = "deque_copy",
        exchange: str = "upbit",
    ):
        """
        ZMQClient 인스턴스를 초기화합니다.

        Args:
            client_id (str): 게이트웨이에서 클라이언트를 식별하기 위한 고유 ID.
            symbol (str): 거래할 자산의 심볼 (예: "KRW-BTC").
            intervals (List[str]): 구독할 캔들의 시간 간격 리스트 (예: ["1m", "5m"]).
                최소 하나의 지원 interval이 필요합니다.
            candle_handler_callback (Callable): 새로운 캔들 데이터가 수신될 때마다 호출될 콜백 함수.
                                                이 함수는 `candle_deques` 딕셔너리를 인자로 받습니다.
            throttle_seconds (Optional[float]): 콜백 함수 호출을 제한하는 시간(초).
                                                None이나 0으로 설정하면 제한 없이 즉시 호출됩니다.
                                                기본값은 0.1초입니다.
            zmq_gateway_host (str): ZMQ 게이트웨이 호스트.
            zmq_gateway_req_port (int): ZMQ REQ 포트.
            zmq_gateway_pub_port (int): ZMQ PUB 포트.
            server_candle_ttl (int): 서버 캔들 구독 TTL (초). 0보다 커야 하며 기본값은 300.
            candle_deque_maxlen (int): 캔들 덱 최대 길이. 0보다 커야 하며 기본값 200.
            on_critical (Optional[Callable[[str], None]]): 치명적 이벤트 시 호출될 콜백.
            callback_snapshot_mode (str): 콜백에 전달할 스냅샷 모드.
                "live": 내부 deque를 그대로 전달
                "deque_copy": deque 컨테이너만 복사 (원소는 공유)
                "deep_copy": 원소 dict까지 복사
            exchange (str): 거래소 이름 (예: "upbit"). 토픽/요청에 사용됩니다.
        """
        self.client_id = client_id
        self.symbol = symbol
        self.intervals = self._validate_intervals(intervals)
        self.exchange = exchange.lower().strip()
        self.exchange_prefix = self.exchange.upper()
        self.candle_handler_callback = candle_handler_callback
        self.throttle_seconds = throttle_seconds
        self.zmq_gateway_host = zmq_gateway_host
        self.zmq_gateway_req_port = zmq_gateway_req_port
        self.zmq_gateway_pub_port = zmq_gateway_pub_port
        if server_candle_ttl <= 0:
            raise ValueError("server_candle_ttl must be greater than 0.")
        self.server_candle_ttl = server_candle_ttl
        if candle_deque_maxlen <= 0:
            raise ValueError("candle_deque_maxlen must be greater than 0.")
        self.candle_deque_maxlen = candle_deque_maxlen
        self.on_critical = on_critical
        if callback_snapshot_mode not in {"live", "deque_copy", "deep_copy"}:
            raise ValueError(
                "Invalid callback_snapshot_mode. "
                "Expected one of: live, deque_copy, deep_copy."
            )
        self.callback_snapshot_mode = callback_snapshot_mode

        self.stop_event = threading.Event()
        self.storage_lock = threading.Lock()
        self._lifecycle_lock = threading.Lock()
        self._subscription_request_lock = threading.Lock()
        self.data_updated_event = threading.Event()
        self._state = ClientState.CREATED
        self._startup_complete_event = threading.Event()
        self._startup_complete_event.set()
        self._stop_complete_event = threading.Event()

        self._transport_connected = threading.Event()
        self._resync_event = threading.Event()
        self._pending_events: Dict[str, List[CandleEvent]] = {}
        self._stream_state: Dict[str, tuple[str, int]] = {}
        self._requested_stream_id: Optional[str] = None
        self._retired_stream_ids = set()
        self._resync_generation = 0
        self._listener_cancel = threading.Event()

        self.consecutive_renewal_failures = 0

        self.candle_deques: Dict[str, deque] = {
            interval: deque(maxlen=self.candle_deque_maxlen)
            for interval in self.intervals
        }
        self.threads: List[threading.Thread] = []

        # Allocate the external resource only after pure validation and state setup.
        self.context = zmq.Context()

    @staticmethod
    def _validate_intervals(intervals: List[str]) -> List[str]:
        if not intervals:
            raise ValueError("intervals must contain at least one supported interval.")

        allowed = {"1m", "3m", "5m", "15m", "30m", "1h", "4h"}
        validated: List[str] = []
        for interval in intervals:
            normalized = interval.strip()
            if normalized not in allowed:
                raise ValueError(
                    "Invalid interval. Expected one of: 1m, 3m, 5m, 15m, 30m, 1h, 4h."
                )
            validated.append(normalized)
        return list(dict.fromkeys(validated))

    def _build_callback_payload(self) -> Dict[str, deque]:
        with self.storage_lock:
            if self.callback_snapshot_mode == "live":
                return self.candle_deques
            if self.callback_snapshot_mode == "deque_copy":
                return {
                    k: deque(v, maxlen=v.maxlen) for k, v in self.candle_deques.items()
                }
            return {
                k: deque((dict(c) for c in v), maxlen=v.maxlen)
                for k, v in self.candle_deques.items()
            }

    def _candle_request(
        self, action: str, interval: str, *, history_count: Optional[int] = None
    ) -> Dict[str, Any]:
        request: Dict[str, Any] = {
            "action": action,
            "symbol": self.symbol,
            "interval": interval,
        }
        if history_count is not None:
            request["history_count"] = history_count
        request["exchange"] = self.exchange
        return request

    def _send_request(
        self,
        request: dict,
        max_retries: int = 5,
        initial_delay: int = 2,
        receive_timeout_ms: int = 10000,
    ) -> GatewayRequestResult:
        """
        ZMQ REQ 소켓을 사용해 게이트웨이에 동기식 요청을 보내고 응답을 받습니다.

        Args:
            request (dict): 게이트웨이로 전송할 요청 메시지 (JSON 직렬화 가능해야 함).
            max_retries (int): 최대 요청 시도 횟수.
            initial_delay (int): 재시도 전 초기 대기 시간(초).
            receive_timeout_ms (int): 각 응답을 기다릴 최대 시간(밀리초).

        Returns:
            GatewayRequestResult: 명시적인 성공 또는 오류 코드와 응답 데이터.
        """
        try:
            encoded_request = orjson.dumps(request)
        except (TypeError, ValueError) as e:
            logger.error(f"ZMQ 요청 직렬화 중 오류 발생: {e}")
            return GatewayRequestResult.failure(GatewayRequestCode.INVALID_REQUEST)

        delay = initial_delay
        last_error_code = GatewayRequestCode.TRANSPORT_ERROR
        for attempt in range(max_retries):
            if attempt > 0 and self.stop_event.is_set():
                logger.debug("종료 요청으로 ZMQ 요청 재시도를 중단합니다.")
                return GatewayRequestResult.failure(GatewayRequestCode.CANCELED)

            socket_req = None
            try:
                socket_req = self.context.socket(zmq.REQ)
                socket_req.setsockopt(zmq.RCVTIMEO, receive_timeout_ms)
                socket_req.setsockopt(zmq.LINGER, 0)
                socket_req.connect(
                    f"tcp://{self.zmq_gateway_host}:{self.zmq_gateway_req_port}"
                )
                socket_req.send(encoded_request)
                response = orjson.loads(socket_req.recv())
                if not isinstance(response, dict):
                    raise ValueError(
                        "ZMQ gateway response must be a JSON object, "
                        f"got {type(response).__name__}."
                    )
                response_status = response.get("status")
                if response_status == "ok":
                    return GatewayRequestResult.success(
                        data=response.get("data"), response=response
                    )
                if response_status == "error":
                    return GatewayRequestResult.failure(
                        GatewayRequestCode.GATEWAY_REJECTED, response=response
                    )
                raise ValueError("ZMQ gateway response status must be 'ok' or 'error'.")
            except zmq.Again:
                last_error_code = GatewayRequestCode.TIMEOUT
                logger.warning(
                    f"ZMQ 게이트웨이 응답 시간 초과 (시도 {attempt + 1}/{max_retries})."
                )
            except (orjson.JSONDecodeError, TypeError, ValueError) as e:
                last_error_code = GatewayRequestCode.INVALID_RESPONSE
                logger.error(
                    f"ZMQ 응답 검증 중 오류 발생 (시도 {attempt + 1}/{max_retries}): {e}"
                )
            except Exception as e:
                last_error_code = GatewayRequestCode.TRANSPORT_ERROR
                logger.error(
                    f"ZMQ 요청 중 오류 발생 (시도 {attempt + 1}/{max_retries}): {e}"
                )
            finally:
                if socket_req is not None:
                    try:
                        socket_req.close()
                    except Exception as e:
                        logger.error(f"ZMQ 요청 소켓 종료 중 오류 발생: {e}")

            if self.stop_event.is_set():
                logger.debug("종료 요청으로 ZMQ 요청 재시도를 중단합니다.")
                return GatewayRequestResult.failure(GatewayRequestCode.CANCELED)

            if attempt < max_retries - 1:
                logger.debug(f"{delay}초 후 재시도합니다...")
                if self.stop_event.wait(delay):
                    logger.debug("종료 요청으로 ZMQ 요청 재시도를 중단합니다.")
                    return GatewayRequestResult.failure(GatewayRequestCode.CANCELED)
                delay = min(delay * 2, 30)

        logger.error(
            f"최대 재시도 횟수({max_retries}회)를 초과하여 요청에 최종 실패했습니다: {request}"
        )
        return GatewayRequestResult.failure(last_error_code)

    @staticmethod
    def _snapshot_from_result(
        result: GatewayRequestResult,
    ) -> Optional[List[Dict[str, Any]]]:
        if not result.ok or not isinstance(result.data, list) or not result.data:
            return None
        if not all(ZMQClient._valid_candle(candle) for candle in result.data):
            return None
        return sorted({c["ts"]: c for c in result.data}.values(), key=lambda c: c["ts"])

    @staticmethod
    def _format_request_failure(result: GatewayRequestResult) -> str:
        detail = result.code.value
        if (
            result.code is not GatewayRequestCode.GATEWAY_REJECTED
            or not result.response
        ):
            return detail

        message = result.response.get("message")
        if not isinstance(message, str):
            return detail

        normalized = " ".join(message.split())
        sanitized = "".join(char for char in normalized if char.isprintable())[:200]
        return f"{detail}: {sanitized}" if sanitized else detail

    def _parse_candle_event(
        self, topic_str: str, payload: object
    ) -> Optional[CandleEvent]:
        parts = topic_str.split(":")
        if len(parts) != 5:
            return None

        exchange_prefix, channel, symbol, interval, event_type_value = parts
        if exchange_prefix != self.exchange_prefix:
            return None
        if channel != "CANDLE" or symbol != self.symbol:
            return None
        if interval not in self.candle_deques or not isinstance(payload, dict):
            return None

        if not self._valid_ordering(payload, "seq"):
            return None

        try:
            event_type = CandleEventType(event_type_value)
        except ValueError:
            return None

        candle = payload.get("candle")
        if event_type is not CandleEventType.RESYNC_REQUIRED and not self._valid_candle(
            candle
        ):
            return None

        new_candle = None
        if event_type is CandleEventType.CLOSE:
            candidate = payload.get("new")
            if not self._valid_candle(candidate) or candidate["ts"] <= candle["ts"]:
                return None
            new_candle = candidate

        return CandleEvent(
            interval=interval,
            event_type=event_type,
            stream_id=payload["stream_id"],
            seq=payload["seq"],
            candle=candle,
            new_candle=new_candle,
        )

    @staticmethod
    def _valid_candle(candle: object) -> bool:
        return (
            isinstance(candle, dict)
            and isinstance(candle.get("ts"), int)
            and not isinstance(candle["ts"], bool)
            and candle["ts"] >= 0
            and isinstance(candle.get("volume", 0), (int, float))
            and candle.get("volume", 0) >= 0
        )

    @staticmethod
    def _valid_ordering(payload, sequence_key):
        return (
            isinstance(payload, Mapping)
            and isinstance(payload.get("stream_id"), str)
            and bool(payload["stream_id"])
            and isinstance(payload.get(sequence_key), int)
            and not isinstance(payload[sequence_key], bool)
            and payload[sequence_key] >= 0
        )

    def _request_resync_locked(self, intervals=None, stream_id=None):
        self._resync_generation += 1
        if stream_id is not None:
            self._requested_stream_id = stream_id
        for interval in intervals or self.intervals:
            self._pending_events.setdefault(interval, [])
        self._resync_event.set()

    def _request_resync(self, intervals: Optional[List[str]] = None) -> None:
        with self.storage_lock:
            self._request_resync_locked(intervals)

    def _apply_candle_event_locked(self, event: CandleEvent) -> bool:
        candles = self.candle_deques[event.interval]
        before = list(candles)
        rows = {candle["ts"]: candle for candle in candles}
        if (
            event.event_type is not CandleEventType.RECONCILE
            or event.candle["ts"] in rows
        ):
            rows[event.candle["ts"]] = event.candle
        if event.new_candle is not None:
            rows[event.new_candle["ts"]] = event.new_candle
        candles.clear()
        candles.extend(sorted(rows.values(), key=lambda candle: candle["ts"]))
        return list(candles) != before

    def _handle_candle_event(self, topic_str: str, payload: dict):
        event = self._parse_candle_event(topic_str, payload)
        if event is None:
            return
        with self.storage_lock:
            if event.stream_id in self._retired_stream_ids:
                return
            state = self._stream_state.get(event.interval)
            if state is not None:
                stream_id, seq = state
                if event.stream_id == stream_id and event.seq <= seq:
                    return
                if (
                    event.stream_id != stream_id
                    and event.stream_id != self._requested_stream_id
                ):
                    self._request_resync_locked(stream_id=event.stream_id)
            pending = self._pending_events.get(event.interval)
            if pending is not None:
                if len(pending) >= self._MAX_PENDING_EVENTS:
                    pending.clear()
                    self._resync_generation += 1
                    self._resync_event.set()
                pending.append(event)
                return
            self._stream_state[event.interval] = (event.stream_id, event.seq)
            if event.event_type is CandleEventType.RESYNC_REQUIRED:
                self._request_resync_locked([event.interval])
            elif self._apply_candle_event_locked(event):
                self.data_updated_event.set()

    def _load_snapshots(self) -> bool:
        with self.storage_lock:
            intervals = list(self._pending_events)
            generation = self._resync_generation
        snapshots = {}
        for interval in intervals:
            with self._subscription_request_lock:
                if self.stop_event.is_set():
                    return False
                result = self._send_request(
                    self._candle_request(
                        "subscribe_candle",
                        interval,
                        history_count=min(
                            self.candle_deque_maxlen, self._MAX_INITIAL_HISTORY_COUNT
                        ),
                    )
                )
            snapshot = self._snapshot_from_result(result)
            if snapshot is None or not self._valid_ordering(
                result.response, "snapshot_seq"
            ):
                return False
            snapshots[interval] = (
                snapshot,
                result.response["stream_id"],
                result.response["snapshot_seq"],
            )
        with self.storage_lock:
            if self.stop_event.is_set() or generation != self._resync_generation:
                return False
            # Validate every interval before publishing any part of the recovery.
            runtimes = {stream_id for _, stream_id, _ in snapshots.values()}
            if len(runtimes) != 1:
                return False
            snapshot_stream_id = next(iter(runtimes))
            if set(intervals) != set(self.intervals) and any(
                state[0] != snapshot_stream_id for state in self._stream_state.values()
            ):
                self._request_resync_locked(stream_id=snapshot_stream_id)
                return False
            for interval, (_, stream_id, watermark) in snapshots.items():
                state = self._stream_state.get(interval)
                if (
                    stream_id in self._retired_stream_ids
                    or (
                        self._requested_stream_id is not None
                        and stream_id != self._requested_stream_id
                    )
                    or (
                        state is not None
                        and state[0] == stream_id
                        and watermark < state[1]
                    )
                ):
                    return False
                if any(
                    event.stream_id == stream_id
                    and event.seq > watermark
                    and event.event_type is CandleEventType.RESYNC_REQUIRED
                    for event in self._pending_events[interval]
                ):
                    return False
            changed = False
            for interval, (snapshot, stream_id, watermark) in snapshots.items():
                state = self._stream_state.get(interval)
                if state is not None and state[0] != stream_id:
                    self._retired_stream_ids.add(state[0])
                candles = self.candle_deques[interval]
                before = list(candles)
                candles.clear()
                candles.extend(snapshot)
                for event in sorted(
                    (
                        event
                        for event in self._pending_events.pop(interval)
                        if event.stream_id == stream_id and event.seq > watermark
                    ),
                    key=lambda event: event.seq,
                ):
                    if event.seq > watermark:
                        self._apply_candle_event_locked(event)
                        watermark = event.seq
                self._stream_state[interval] = (stream_id, watermark)
                changed |= list(candles) != before
            self._requested_stream_id = None
            self._resync_event.clear()
            if changed:
                self.data_updated_event.set()
        return True

    def _recovery_thread(self):
        while not self.stop_event.is_set():
            if not self._transport_connected.wait(0.1):
                continue
            if not self._resync_event.wait(0.1):
                continue
            if not self._load_snapshots():
                logger.error(
                    "Candle recovery snapshot failed; retrying with live events buffered."
                )
                if self.on_critical:
                    try:
                        self.on_critical(
                            "Candle recovery snapshot failed; data is not ready."
                        )
                    except Exception:
                        logger.exception("Critical callback failed")
                self.stop_event.wait(1)

    def _data_listener_thread(
        self,
        ready_event: Optional[threading.Event] = None,
        failed_event: Optional[threading.Event] = None,
    ):
        """[스레드 타겟] ZMQ SUB 소켓을 통해 실시간 캔들 데이터를 구독하고 수신합니다."""
        socket_sub = None
        monitor = None
        try:
            try:
                socket_sub = self.context.socket(zmq.SUB)
                monitor = socket_sub.get_monitor_socket(
                    events=zmq.EVENT_CONNECTED | zmq.EVENT_DISCONNECTED
                )
                socket_sub.connect(
                    f"tcp://{self.zmq_gateway_host}:{self.zmq_gateway_pub_port}"
                )

                for interval in self.intervals:
                    topic = f"{self.exchange_prefix}:CANDLE:{self.symbol}:{interval}:"
                    socket_sub.setsockopt_string(zmq.SUBSCRIBE, topic)
                    logger.debug(f"[{self.client_id}][SUB] 토픽 구독: '{topic}'")
            except Exception as e:
                logger.error(f"ZMQ SUB 리스너 초기화 중 오류 발생: {e}")
                if failed_event is not None:
                    failed_event.set()
                if ready_event is not None:
                    ready_event.set()
                return

            if ready_event is not None:
                ready_event.set()

            while not self.stop_event.is_set() and not self._listener_cancel.is_set():
                try:
                    if monitor.poll(0):
                        connection = recv_monitor_message(monitor)
                        if connection["event"] == zmq.EVENT_DISCONNECTED:
                            self._transport_connected.clear()
                            self._request_resync()
                        elif connection["event"] == zmq.EVENT_CONNECTED:
                            self._transport_connected.set()
                    if not socket_sub.poll(10):
                        continue
                    frames = socket_sub.recv_multipart()
                    if not isinstance(frames, (list, tuple)) or len(frames) != 2:
                        logger.warning(
                            f"잘못된 캔들 이벤트 프레임을 무시합니다: {frames!r}"
                        )
                        continue
                    topic_bytes, payload_bytes = frames
                    topic_str = topic_bytes.decode()
                    payload = orjson.loads(payload_bytes)
                except zmq.Again:
                    continue
                except (orjson.JSONDecodeError, UnicodeDecodeError, TypeError) as e:
                    logger.warning(f"잘못된 캔들 이벤트 페이로드를 무시합니다: {e}")
                    continue
                self._handle_candle_event(topic_str, payload)
        finally:
            if monitor is not None:
                monitor.close(linger=0)
            if socket_sub is not None:
                try:
                    socket_sub.close()
                except Exception as e:
                    logger.error(f"ZMQ SUB 리스너 소켓 종료 중 오류 발생: {e}")
            logger.debug(f"\n[{self.client_id}][SUB] 데이터 리스너 종료.")

    def _subscription_renewer_thread(self):
        """[스레드 타겟] 게이트웨이의 구독 TTL이 만료되기 전에 주기적으로 구독을 갱신합니다."""
        renew_interval = self.server_candle_ttl / 2

        while not self.stop_event.wait(renew_interval):
            logger.debug("모든 캔들 구독 갱신을 시작합니다...")

            last_failure_detail = None

            for interval in self.intervals:
                request = self._candle_request(
                    "subscribe_candle", interval, history_count=1
                )
                with self._subscription_request_lock:
                    if self.stop_event.is_set():
                        break
                    response = self._send_request(request)

                if self.stop_event.is_set():
                    break

                if response.ok:
                    if not self._valid_ordering(response.response, "snapshot_seq"):
                        response = GatewayRequestResult.failure(
                            GatewayRequestCode.INVALID_RESPONSE
                        )
                    else:
                        with self.storage_lock:
                            stream_id = response.response["stream_id"]
                            state = self._stream_state.get(interval)
                            if (
                                state is not None
                                and state[0] != stream_id
                                and stream_id not in self._retired_stream_ids
                                and stream_id != self._requested_stream_id
                            ):
                                self._request_resync_locked(stream_id=stream_id)

                if not response.ok:
                    last_failure_detail = self._format_request_failure(response)
                    logger.error(
                        f"❌ [{interval}] 구독 갱신에 최종 실패했습니다! "
                        f"오류: {last_failure_detail}"
                    )

            if self.stop_event.is_set():
                break

            if last_failure_detail is None:
                if self.consecutive_renewal_failures > 0:
                    logger.info(
                        "✅ 구독 갱신이 정상화되었습니다. (이전 연속 실패: "
                        f"{self.consecutive_renewal_failures}회)"
                    )
                self.consecutive_renewal_failures = 0
            else:
                self.consecutive_renewal_failures += 1
                logger.warning(
                    "⚠️ 구독 갱신 주기에 실패가 포함되었습니다. (현재 연속 "
                    f"{self.consecutive_renewal_failures}회 실패)"
                )

                if self.consecutive_renewal_failures >= self.MAX_CONSECUTIVE_FAILURES:
                    critical_msg = (
                        "🚨🚨🚨 [심각] ZMQ 구독 갱신이 "
                        f"{self.consecutive_renewal_failures}회 연속 실패했습니다!\n"
                        f"- 클라이언트 ID: {self.client_id}\n"
                        f"- 최근 오류: {last_failure_detail or 'unknown'}\n"
                        "- 게이트웨이가 구독 갱신 요청을 연속으로 처리하지 못했습니다.\n"
                        "- 오류 코드, 구독 설정, 서버 상태 및 네트워크를 확인하세요."
                    )
                    logger.critical(critical_msg)
                    if self.on_critical:
                        try:
                            self.on_critical(critical_msg)
                        except Exception as e:
                            logger.error(f"치명 이벤트 콜백 실행 중 예외 발생: {e}")
        logger.debug(f"[{self.client_id}][RENEWER] 구독 갱신 스레드 종료.")

    def _strategy_trigger_thread(self):
        """
        [스레드 타겟] 데이터 업데이트 이벤트를 감지하여 전략 콜백 함수를 실행합니다.
        ...
        """
        throttle_seconds = (
            self.throttle_seconds
            if self.throttle_seconds and self.throttle_seconds > 0
            else None
        )
        while not self.stop_event.is_set():
            if throttle_seconds is None:
                updated = self.data_updated_event.wait(timeout=1)
            else:
                if self.stop_event.wait(throttle_seconds):
                    break
                updated = self.data_updated_event.is_set()

            if not updated:
                continue

            self.data_updated_event.clear()
            with self.storage_lock:
                if self._pending_events:
                    continue
            try:
                payload = self._build_callback_payload()
                self.candle_handler_callback(payload)
            except Exception as e:
                logger.error(f"전략 콜백 함수 실행 중 오류: {e}", exc_info=True)

        logger.debug(f"[{self.client_id}][TRIGGER] 전략 트리거 스레드 종료.")

    def _run_worker(self, worker):
        self._startup_complete_event.wait()
        if not self.stop_event.is_set():
            worker()

    def start(self) -> bool:
        """
        클라이언트를 시작합니다.

        SUB 리스너를 먼저 시작하고 스냅샷 로딩 중 이벤트를 버퍼링합니다.
        스냅샷 병합이 완료되면 구독 갱신, 복구, 콜백 스레드를 시작합니다.

        Returns:
            bool: 초기화 및 스레드 시작에 성공하면 True, 실패하면 False.
        """
        with self._lifecycle_lock:
            if self._state is not ClientState.CREATED or self.stop_event.is_set():
                return False
            self._state = ClientState.STARTING
            self._startup_complete_event.clear()

        def abort_start_locked() -> bool:
            if self._state is ClientState.STARTING:
                self._state = ClientState.CREATED
            self._startup_complete_event.set()
            return False

        def abort_start() -> bool:
            with self.storage_lock:
                self._pending_events.clear()
                self._stream_state.clear()
                self._requested_stream_id = None
                self._resync_event.clear()
                for candles in self.candle_deques.values():
                    candles.clear()
            with self._lifecycle_lock:
                return abort_start_locked()

        logger.info("ZMQ 게이트웨이에 연결 및 스냅샷 요청을 시작합니다...")

        self._listener_cancel.clear()
        self._request_resync()

        listener_ready = threading.Event()
        listener_failed = threading.Event()
        listener = threading.Thread(
            target=self._data_listener_thread,
            args=(listener_ready, listener_failed),
            name="ZMQListener",
        )
        try:
            listener.start()
        except Exception as e:
            logger.error(f"ZMQ SUB 리스너 스레드 시작 중 오류 발생: {e}")
            return abort_start()
        listener_ready.wait()

        if listener_failed.is_set():
            listener.join(timeout=1)
            logger.error(
                "ZMQ SUB 리스너를 초기화하지 못해 클라이언트를 시작할 수 없습니다."
            )
            return abort_start()

        prepared_threads = [listener]

        def rollback_prepared_threads() -> None:
            self._listener_cancel.set()
            self._startup_complete_event.set()
            current_thread = threading.current_thread()
            for thread in prepared_threads:
                if thread is not current_thread and thread.is_alive():
                    thread.join(timeout=5)

        if not self._load_snapshots():
            rollback_prepared_threads()
            return abort_start()

        try:
            renewer = threading.Thread(
                target=self._run_worker,
                args=(self._subscription_renewer_thread,),
                name="SubscriptionRenewer",
            )
            trigger = threading.Thread(
                target=self._run_worker,
                args=(self._strategy_trigger_thread,),
                name="StrategyTrigger",
            )
            recovery = threading.Thread(
                target=self._run_worker,
                args=(self._recovery_thread,),
                name="CandleRecovery",
            )
            for thread in (renewer, trigger, recovery):
                if self.stop_event.is_set():
                    rollback_prepared_threads()
                    return abort_start()
                thread.start()
                prepared_threads.append(thread)
        except Exception as e:
            logger.error(f"ZMQ 백그라운드 스레드 시작 중 오류 발생: {e}")
            self.stop_event.set()
            rollback_prepared_threads()
            return abort_start()

        # Publish snapshots, registered threads, and RUNNING atomically against stop().
        startup_canceled = False
        with self.storage_lock:
            with self._lifecycle_lock:
                if self._state is not ClientState.STARTING or self.stop_event.is_set():
                    startup_canceled = True
                else:
                    self.threads.extend(prepared_threads)
                    self._state = ClientState.RUNNING
                    self._startup_complete_event.set()

        if startup_canceled:
            rollback_prepared_threads()
            return abort_start()

        logger.info("✅ 모든 스냅샷 수신 완료. 실시간 분석을 시작합니다.")
        return True

    def _publish_stop_complete(self) -> None:
        with self._lifecycle_lock:
            self._state = ClientState.STOPPED
            self._stop_complete_event.set()
        logger.info("ZMQ 클라이언트가 성공적으로 종료되었습니다.")

    def _publish_stop_complete_after(self, thread: threading.Thread) -> None:
        try:
            thread.join()
        except BaseException:
            with self._lifecycle_lock:
                self._state = ClientState.STOP_FAILED
                self._stop_complete_event.set()
            logger.exception("ZMQ 종료 소유 스레드 대기에 실패했습니다.")
            return
        self._publish_stop_complete()

    def _perform_stop_cleanup(
        self, completion_thread: Optional[threading.Thread] = None
    ) -> None:
        logger.info("ZMQ 클라이언트 종료 절차 시작...")

        try:
            with self._subscription_request_lock:
                for interval in self.intervals:
                    logger.debug(f"[{interval}] 구독 해지 요청 중...")
                    try:
                        self._send_request(
                            self._candle_request("unsubscribe_candle", interval),
                            max_retries=1,
                            receive_timeout_ms=self._STOP_UNSUBSCRIBE_TIMEOUT_MS,
                        )
                    except Exception as e:
                        logger.error(f"[{interval}] 구독 해지 요청 중 오류 발생: {e}")

            current_thread = threading.current_thread()
            for thread in self.threads:
                if thread is not current_thread and thread.is_alive():
                    thread.join()

            self.context.term()

            if completion_thread is not None:
                finalizer = threading.Thread(
                    target=self._publish_stop_complete_after,
                    args=(completion_thread,),
                    name="ZMQStopFinalizer",
                    daemon=False,
                )
                finalizer.start()
                return
        except BaseException:
            with self._lifecycle_lock:
                self._state = ClientState.STOP_FAILED
                self._stop_complete_event.set()
            raise

        self._publish_stop_complete()

    def stop(self):
        """
        클라이언트를 안전하게 종료합니다.

        모든 백그라운드 스레드에 종료 신호를 보내고, 게이트웨이에 구독 해지를
        요청한 후, 스레드가 종료될 때까지 대기합니다.
        """
        caller = threading.current_thread()
        startup_complete_event = None

        while True:
            with self._lifecycle_lock:
                if self._state is ClientState.STOPPED:
                    return
                if self._state is ClientState.STOPPING:
                    if caller in self.threads:
                        return
                    stop_complete_event = self._stop_complete_event
                else:
                    if self._state is ClientState.STARTING:
                        startup_complete_event = self._startup_complete_event
                    self._state = ClientState.STOPPING
                    self._stop_complete_event.clear()
                    self.stop_event.set()
                    break

            stop_complete_event.wait()

        if startup_complete_event is not None:
            startup_complete_event.wait()

        completion_thread = caller if caller in self.threads else None
        self._perform_stop_cleanup(completion_thread)
