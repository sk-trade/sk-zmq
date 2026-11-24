"""End-to-end request/subscription handoff over real TCP sockets."""

from __future__ import annotations

import threading
from unittest.mock import patch

import pytest
import zmq

from sk_zmq import ZMQClient

from ._helpers import RESTART, STREAM, _make_candle, _request_failure


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
    original = client._ingest_publication

    def receive(topic, payload):
        original(topic, payload)
        if topic.endswith(":CLOSE"):
            live_received.set()

    try:
        with patch.object(client, "_ingest_publication", side_effect=receive):
            assert client.start()
            assert client.is_running
            assert [c["ts"] for c in client._build_callback_snapshot()["15m"]] == [
                1,
                2,
                3,
            ]
            begin_recovery.set()
            assert recovered.wait(3)
            candles = list(client._build_callback_snapshot()["15m"])
            assert [c["ts"] for c in candles] == [3, 4, 5]
            assert [c["close"] for c in candles] == [130, 104, 105]
            client.stop()
            client.stop()
            assert not client.is_running
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
        assert all(not thread.is_alive() for thread in client._worker_threads)
    finally:
        if not client._stop_requested.is_set():
            with patch.object(
                client, "_request_gateway", return_value=_request_failure()
            ):
                client.stop()
        gateway.join(4)
        context.term()
