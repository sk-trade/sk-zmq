import logging
import threading
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Mapping, Optional

import orjson
import zmq

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


@dataclass(frozen=True)
class CandleEvent:
    interval: str
    event_type: CandleEventType
    candle: Dict[str, Any]
    new_candle: Optional[Dict[str, Any]] = None


class ClientState(Enum):
    CREATED = "created"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOP_FAILED = "stop_failed"
    STOPPED = "stopped"


class ListenerStartCode(Enum):
    PENDING = "pending"
    READY = "ready"
    FAILED = "failed"


@dataclass(frozen=True)
class ListenerStartResult:
    code: ListenerStartCode
    cause: Optional[Exception] = None


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
            candle_handler_callback (Callable): 새로운 캔들 데이터가 수신될 때마다 호출될 콜백 함수.
                                                이 함수는 `candle_deques` 딕셔너리를 인자로 받습니다.
            throttle_seconds (Optional[float]): 콜백 함수 호출을 제한하는 시간(초).
                                                None이나 0으로 설정하면 제한 없이 즉시 호출됩니다.
                                                기본값은 0.1초입니다.
            zmq_gateway_host (str): ZMQ 게이트웨이 호스트.
            zmq_gateway_req_port (int): ZMQ REQ 포트.
            zmq_gateway_pub_port (int): ZMQ PUB 포트.
            server_candle_ttl (int): 서버 캔들 구독 TTL (초). 0보다 커야 하며 기본값은 300.
            candle_deque_maxlen (int): 캔들 덱 최대 길이. 기본값 200.
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
        self.candle_deque_maxlen = candle_deque_maxlen
        self.on_critical = on_critical
        self.callback_snapshot_mode = callback_snapshot_mode
        self._validate_callback_snapshot_mode()

        self.stop_event = threading.Event()
        self.storage_lock = threading.Lock()
        self._lifecycle_lock = threading.Lock()
        self._subscription_request_lock = threading.Lock()
        self.data_updated_event = threading.Event()
        self._state = ClientState.CREATED
        self._startup_complete_event = threading.Event()
        self._startup_complete_event.set()
        self._stop_complete_event = threading.Event()

        self.consecutive_renewal_failures = 0
        self.MAX_CONSECUTIVE_FAILURES = 3
        self._listener_start_result = ListenerStartResult(ListenerStartCode.PENDING)

        self.candle_deques: Dict[str, deque] = {
            interval: deque(maxlen=self.candle_deque_maxlen) for interval in self.intervals
        }
        self.threads: List[threading.Thread] = []

        # Allocate the external resource only after pure validation and state setup.
        self.context = zmq.Context()

    def _validate_intervals(self, intervals: List[str]) -> List[str]:
        allowed = {"1m", "3m", "5m", "10m", "30m", "1h", "4h", "1d"}
        validated: List[str] = []
        for interval in intervals:
            normalized = interval.strip()
            if normalized not in allowed:
                raise ValueError(
                    "Invalid interval. Expected one of: 1m, 3m, 5m, 10m, 30m, 1h, 4h, 1d."
                )
            validated.append(normalized)
        return validated

    def _validate_callback_snapshot_mode(self) -> None:
        valid_modes = {"live", "deque_copy", "deep_copy"}
        if self.callback_snapshot_mode not in valid_modes:
            raise ValueError(
                "Invalid callback_snapshot_mode. "
                "Expected one of: live, deque_copy, deep_copy."
            )

    def _build_callback_payload(self) -> Dict[str, deque]:
        with self.storage_lock:
            if self.callback_snapshot_mode == "live":
                return self.candle_deques
            if self.callback_snapshot_mode == "deque_copy":
                return {
                    k: deque(v, maxlen=v.maxlen) for k, v in self.candle_deques.items()
                }
            return {
                k: deque([dict(c) for c in v], maxlen=v.maxlen)
                for k, v in self.candle_deques.items()
            }

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
        if not all(isinstance(candle, dict) for candle in result.data):
            return None
        return result.data

    @staticmethod
    def _format_request_failure(result: GatewayRequestResult) -> str:
        detail = result.code.value
        if result.code is not GatewayRequestCode.GATEWAY_REJECTED or not result.response:
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

        try:
            event_type = CandleEventType(event_type_value)
        except ValueError:
            return None

        candle = payload.get("candle")
        if not isinstance(candle, dict):
            return None
        if event_type is CandleEventType.RECONCILE and candle.get("ts") is None:
            return None

        new_candle = None
        if event_type is CandleEventType.CLOSE:
            candidate = payload.get("new")
            if not isinstance(candidate, dict):
                return None
            new_candle = candidate

        return CandleEvent(
            interval=interval,
            event_type=event_type,
            candle=candle,
            new_candle=new_candle,
        )

    def _handle_candle_event(self, topic_str: str, payload: dict):
        """
        수신된 캔들 이벤트를 파싱하고 내부 데이터 저장소(`candle_deques`)를 업데이트합니다.

        이벤트 유형:
        - UPDATE: 현재 진행 중인 캔들의 정보 (시가, 고가, 저가, 종가)가 업데이트됨.
        - CLOSE: 캔들이 마감되고 새로운 캔들이 시작됨.
        - RECONCILE: 거래소의 최종 데이터와 불일치가 있을 경우, 과거 캔들 데이터가 보정됨.

        Args:
            topic_str (str): 이벤트가 발생한 ZMQ 토픽 문자열.
            payload (dict): 이벤트와 관련된 데이터.
        """
        event = self._parse_candle_event(topic_str, payload)
        if event is None:
            return

        target_deque = self.candle_deques[event.interval]
        updated = False

        with self.storage_lock:
            if event.event_type is CandleEventType.UPDATE:
                if target_deque:
                    if target_deque[-1] == event.candle:
                        return
                    target_deque[-1] = event.candle
                else:
                    target_deque.append(event.candle)
                updated = bool(target_deque)
            elif event.event_type is CandleEventType.CLOSE:
                previous_candles = list(target_deque)
                if target_deque:
                    target_deque[-1] = event.candle
                else:
                    target_deque.append(event.candle)
                target_deque.append(event.new_candle)
                updated = list(target_deque) != previous_candles
            elif event.event_type is CandleEventType.RECONCILE:
                reconciled_ts = event.candle["ts"]
                for i in range(len(target_deque) - 1, -1, -1):
                    if target_deque[i].get("ts") == reconciled_ts:
                        if target_deque[i] == event.candle:
                            break
                        logger.debug(
                            f"[{event.interval}] 캔들 데이터 보정 발생! ts:{reconciled_ts}"
                        )
                        target_deque[i] = event.candle
                        updated = True
                        break

        if updated:
            self.data_updated_event.set()

    def _data_listener_thread(
        self,
        ready_event: Optional[threading.Event] = None,
        run_event: Optional[threading.Event] = None,
    ):
        """[스레드 타겟] ZMQ SUB 소켓을 통해 실시간 캔들 데이터를 구독하고 수신합니다."""
        socket_sub = None
        try:
            try:
                socket_sub = self.context.socket(zmq.SUB)
                socket_sub.connect(
                    f"tcp://{self.zmq_gateway_host}:{self.zmq_gateway_pub_port}"
                )

                for interval in self.intervals:
                    topic = f"{self.exchange_prefix}:CANDLE:{self.symbol}:{interval}:"
                    socket_sub.setsockopt_string(zmq.SUBSCRIBE, topic)
                    logger.debug(f"[{self.client_id}][SUB] 토픽 구독: '{topic}'")
            except Exception as e:
                self._listener_start_result = ListenerStartResult(
                    ListenerStartCode.FAILED, cause=e
                )
                logger.error(f"ZMQ SUB 리스너 초기화 중 오류 발생: {e}")
                if ready_event is not None:
                    ready_event.set()
                return

            self._listener_start_result = ListenerStartResult(ListenerStartCode.READY)
            if ready_event is not None:
                ready_event.set()

            if run_event is not None:
                while not run_event.wait(timeout=0.1):
                    if self.stop_event.is_set():
                        return

            while not self.stop_event.is_set():
                try:
                    frames = socket_sub.recv_multipart(flags=zmq.NOBLOCK)
                    if not isinstance(frames, (list, tuple)) or len(frames) != 2:
                        logger.warning(f"잘못된 캔들 이벤트 프레임을 무시합니다: {frames!r}")
                        continue
                    topic_bytes, payload_bytes = frames
                    topic_str = topic_bytes.decode()
                    payload = orjson.loads(payload_bytes)
                except zmq.Again:
                    time.sleep(0.01)
                    continue
                except (orjson.JSONDecodeError, UnicodeDecodeError, TypeError) as e:
                    logger.warning(f"잘못된 캔들 이벤트 페이로드를 무시합니다: {e}")
                    continue
                self._handle_candle_event(topic_str, payload)
        finally:
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

            all_renewals_succeeded = True
            renewal_cycle_canceled = False
            last_failure_detail = None

            for interval in self.intervals:
                request = {
                    "action": "subscribe_candle",
                    "symbol": self.symbol,
                    "interval": interval,
                    "history_count": 1,
                    "exchange": self.exchange,
                }
                with self._subscription_request_lock:
                    if self.stop_event.is_set():
                        renewal_cycle_canceled = True
                        break
                    response = self._send_request(request)

                if self.stop_event.is_set():
                    renewal_cycle_canceled = True
                    break

                if not response.ok:
                    all_renewals_succeeded = False
                    last_failure_detail = self._format_request_failure(response)
                    logger.error(
                        f"❌ [{interval}] 구독 갱신에 최종 실패했습니다! "
                        f"오류: {last_failure_detail}"
                    )

            if renewal_cycle_canceled or self.stop_event.is_set():
                break

            if all_renewals_succeeded:
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
        if self.throttle_seconds and self.throttle_seconds > 0:
            while not self.stop_event.wait(self.throttle_seconds):
                if self.data_updated_event.is_set():
                    self.data_updated_event.clear()
                    try:
                        payload = self._build_callback_payload()
                        self.candle_handler_callback(payload)
                    except Exception as e:
                        logger.error(f"전략 콜백 함수 실행 중 오류: {e}", exc_info=True)
        else:
            while not self.stop_event.is_set():
                if self.data_updated_event.wait(timeout=1):
                    self.data_updated_event.clear()
                    try:
                        payload = self._build_callback_payload()
                        self.candle_handler_callback(payload)
                    except Exception as e:
                        logger.error(f"전략 콜백 함수 실행 중 오류: {e}", exc_info=True)

        logger.debug(f"[{self.client_id}][TRIGGER] 전략 트리거 스레드 종료.")

    def start(self) -> bool:
        """
        클라이언트를 시작합니다.

        초기 캔들 데이터(스냅샷)를 요청하고, 성공적으로 수신하면
        데이터 수신, 구독 갱신, 전략 실행을 위한 백그라운드 스레드를 시작합니다.

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
            with self._lifecycle_lock:
                return abort_start_locked()

        logger.info("ZMQ 게이트웨이에 연결 및 스냅샷 요청을 시작합니다...")

        initial_snapshots: Dict[str, list] = {}
        for interval in self.intervals:
            req = {
                "action": "subscribe_candle",
                "symbol": self.symbol,
                "interval": interval,
                "history_count": self.candle_deque_maxlen,
                "exchange": self.exchange,
            }
            with self._subscription_request_lock:
                if self.stop_event.is_set():
                    return abort_start()
                response = self._send_request(req)

            if self.stop_event.is_set():
                return abort_start()

            snapshot = self._snapshot_from_result(response)
            if snapshot is None:
                logger.error(f"❌ [{interval}] 스냅샷 수신 실패. 클라이언트를 시작할 수 없습니다.")
                return abort_start()
            initial_snapshots[interval] = snapshot
            logger.debug(f"✅ [{interval}] 스냅샷 수신 성공 ({len(snapshot)}개).")

        listener_ready = threading.Event()
        listener_run = threading.Event()
        self._listener_start_result = ListenerStartResult(ListenerStartCode.PENDING)
        listener = threading.Thread(
            target=self._data_listener_thread,
            args=(listener_ready, listener_run),
            name="ZMQListener",
        )
        try:
            listener.start()
        except Exception as e:
            logger.error(f"ZMQ SUB 리스너 스레드 시작 중 오류 발생: {e}")
            return abort_start()
        listener_ready.wait()

        if self._listener_start_result.code is not ListenerStartCode.READY:
            listener.join(timeout=1)
            logger.error(
                "ZMQ SUB 리스너를 초기화하지 못해 클라이언트를 시작할 수 없습니다."
            )
            return abort_start()

        prepared_threads = [listener]

        def rollback_prepared_threads() -> None:
            listener_run.set()
            current_thread = threading.current_thread()
            for thread in prepared_threads:
                if thread is not current_thread and thread.is_alive():
                    thread.join(timeout=5)

        try:
            renewer = threading.Thread(
                target=self._subscription_renewer_thread,
                name="SubscriptionRenewer",
            )
            trigger = threading.Thread(
                target=self._strategy_trigger_thread,
                name="StrategyTrigger",
            )
            for thread in (renewer, trigger):
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
                    for interval, snapshot in initial_snapshots.items():
                        self.candle_deques[interval].extend(snapshot)

                    self.threads.extend(prepared_threads)
                    self._state = ClientState.RUNNING
                    self._startup_complete_event.set()
                    listener_run.set()

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
                            {
                                "action": "unsubscribe_candle",
                                "symbol": self.symbol,
                                "interval": interval,
                                "exchange": self.exchange,
                            },
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
