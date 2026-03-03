import logging
import threading
import time
from collections import deque
from typing import Callable, Dict, List, Optional

import orjson
import zmq

logger = logging.getLogger(__name__)


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

    def __init__(
        self,
        client_id: str,
        symbol: str,
        intervals: List[str],
        candle_handler_callback: Callable[[Dict[str, deque]], None],
        throttle_seconds: Optional[float] = 0.1,
        *,
        zmq_gateway_host: str,
        zmq_gateway_req_port: int,
        zmq_gateway_pub_port: int,
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
            server_candle_ttl (int): 서버 캔들 구독 TTL (초). 기본값 300.
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
        self.server_candle_ttl = server_candle_ttl
        self.candle_deque_maxlen = candle_deque_maxlen
        self.on_critical = on_critical
        self.callback_snapshot_mode = callback_snapshot_mode

        self.context = zmq.Context()
        self.stop_event = threading.Event()
        self.storage_lock = threading.Lock()
        self.data_updated_event = threading.Event()

        self.consecutive_renewal_failures = 0
        self.MAX_CONSECUTIVE_FAILURES = 3

        self.candle_deques: Dict[str, deque] = {
            interval: deque(maxlen=self.candle_deque_maxlen) for interval in self.intervals
        }
        self._validate_callback_snapshot_mode()
        self.threads: List[threading.Thread] = []

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

    def _send_request(self, request: dict, max_retries: int = 5, initial_delay: int = 2) -> Optional[dict]:
        """
        ZMQ REQ 소켓을 사용해 게이트웨이에 동기식 요청을 보내고 응답을 받습니다.

        Args:
            request (dict): 게이트웨이로 전송할 요청 메시지 (JSON 직렬화 가능해야 함).

        Returns:
            Optional[dict]: 게이트웨이로부터 받은 응답. 타임아웃 또는 오류 발생 시 None을 반환.
        """
        delay = initial_delay
        for attempt in range(max_retries):
            socket_req = self.context.socket(zmq.REQ)
            socket_req.setsockopt(zmq.RCVTIMEO, 10000)
            socket_req.setsockopt(zmq.LINGER, 0)
            socket_req.connect(f"tcp://{self.zmq_gateway_host}:{self.zmq_gateway_req_port}")

            try:
                socket_req.send(orjson.dumps(request))
                response = orjson.loads(socket_req.recv())
                return response
            except zmq.Again:
                logger.warning(
                    f"ZMQ 게이트웨이 응답 시간 초과 (시도 {attempt + 1}/{max_retries})."
                )
            except Exception as e:
                logger.error(
                    f"ZMQ 요청 중 오류 발생 (시도 {attempt + 1}/{max_retries}): {e}"
                )
            finally:
                socket_req.close()

            if attempt < max_retries - 1:
                logger.info(f"{delay}초 후 재시도합니다...")
                time.sleep(delay)
                delay = min(delay * 2, 30)

        logger.error(
            f"최대 재시도 횟수({max_retries}회)를 초과하여 요청에 최종 실패했습니다: {request}"
        )
        return None

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
        parts = topic_str.split(":")
        if len(parts) < 5:
            return

        exchange_prefix, channel, symbol, interval, event_type = parts[:5]
        if exchange_prefix != self.exchange_prefix:
            return
        if channel != "CANDLE" or symbol != self.symbol:
            return
        if interval not in self.candle_deques:
            return

        target_deque = self.candle_deques[interval]

        with self.storage_lock:
            if event_type == "UPDATE":
                candle = payload.get("candle")
                if candle is None:
                    return
                if target_deque:
                    target_deque[-1] = candle
                else:
                    target_deque.append(candle)
            elif event_type == "CLOSE":
                candle = payload.get("candle")
                new_candle = payload.get("new")
                if candle is None or new_candle is None:
                    return
                if target_deque:
                    target_deque[-1] = candle
                else:
                    target_deque.append(candle)
                target_deque.append(new_candle)
            elif event_type == "RECONCILE":
                reconciled_candle = payload.get("candle")
                if reconciled_candle is None:
                    return
                reconciled_ts = reconciled_candle.get("ts")
                if reconciled_ts is None:
                    return
                for i in range(len(target_deque) - 1, -1, -1):
                    if target_deque[i].get("ts") == reconciled_ts:
                        logger.info(
                            f"\n[INFO] [{interval}] 캔들 데이터 보정 발생! ts:{reconciled_ts}"
                        )
                        target_deque[i] = reconciled_candle
                        break

        self.data_updated_event.set()

    def _data_listener_thread(self):
        """[스레드 타겟] ZMQ SUB 소켓을 통해 실시간 캔들 데이터를 구독하고 수신합니다."""
        socket_sub = self.context.socket(zmq.SUB)
        socket_sub.connect(f"tcp://{self.zmq_gateway_host}:{self.zmq_gateway_pub_port}")

        for interval in self.intervals:
            topic = f"{self.exchange_prefix}:CANDLE:{self.symbol}:{interval}:"
            socket_sub.setsockopt_string(zmq.SUBSCRIBE, topic)
            logger.info(f"[{self.client_id}][SUB] 토픽 구독: '{topic}'")

        while not self.stop_event.is_set():
            try:
                topic_bytes, payload_bytes = socket_sub.recv_multipart(flags=zmq.NOBLOCK)
                self._handle_candle_event(topic_bytes.decode(), orjson.loads(payload_bytes))
            except zmq.Again:
                time.sleep(0.01)
        socket_sub.close()
        logger.debug(f"\n[{self.client_id}][SUB] 데이터 리스너 종료.")

    def _subscription_renewer_thread(self):
        """[스레드 타겟] 게이트웨이의 구독 TTL이 만료되기 전에 주기적으로 구독을 갱신합니다."""
        renew_interval = (self.server_candle_ttl / 2) - 10
        if renew_interval < 10:
            renew_interval = 10

        while not self.stop_event.wait(renew_interval):
            logger.info("모든 캔들 구독 갱신을 시작합니다...")

            all_renewals_succeeded = True

            for interval in self.intervals:
                request = {
                    "action": "subscribe_candle",
                    "symbol": self.symbol,
                    "interval": interval,
                    "exchange": self.exchange,
                }
                response = self._send_request(request)

                if not (response and response.get("status") == "ok"):
                    all_renewals_succeeded = False
                    logger.error(f"❌ [{interval}] 구독 갱신에 최종 실패했습니다! 응답: {response}")

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
                        "- 서버와의 연결이 끊겼을 가능성이 매우 높습니다.\n"
                        "- 데이터 수신이 중단되었을 수 있으니 서버 및 네트워크 상태를 즉시 확인하세요."
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
        logger.info("ZMQ 게이트웨이에 연결 및 스냅샷 요청을 시작합니다...")

        for interval in self.intervals:
            req = {
                "action": "subscribe_candle",
                "symbol": self.symbol,
                "interval": interval,
                "history_count": self.candle_deque_maxlen,
                "exchange": self.exchange,
            }
            response = self._send_request(req)

            if response and response.get("status") == "ok" and response.get("data"):
                with self.storage_lock:
                    self.candle_deques[interval].extend(response["data"])
                logger.info(f"✅ [{interval}] 스냅샷 수신 성공 ({len(response['data'])}개).")
            else:
                logger.error(f"❌ [{interval}] 스냅샷 수신 실패. 클라이언트를 시작할 수 없습니다.")
                return False

        listener = threading.Thread(target=self._data_listener_thread, name="ZMQListener")
        renewer = threading.Thread(target=self._subscription_renewer_thread, name="SubscriptionRenewer")
        trigger = threading.Thread(target=self._strategy_trigger_thread, name="StrategyTrigger")
        self.threads.extend([listener, renewer, trigger])
        for t in self.threads:
            t.start()

        logger.info("✅ 모든 스냅샷 수신 완료. 실시간 분석을 시작합니다.")
        return True

    def stop(self):
        """
        클라이언트를 안전하게 종료합니다.

        모든 백그라운드 스레드에 종료 신호를 보내고, 게이트웨이에 구독 해지를
        요청한 후, 스레드가 종료될 때까지 대기합니다.
        """
        logger.info("ZMQ 클라이언트 종료 절차 시작...")
        self.stop_event.set()

        for interval in self.intervals:
            logger.debug(f"[{interval}] 구독 해지 요청 중...")
            self._send_request(
                {
                    "action": "unsubscribe_candle",
                    "symbol": self.symbol,
                    "interval": interval,
                    "exchange": self.exchange,
                }
            )

        for t in self.threads:
            if t.is_alive():
                t.join(timeout=5)

        self.context.term()
        logger.info("ZMQ 클라이언트가 성공적으로 종료되었습니다.")
