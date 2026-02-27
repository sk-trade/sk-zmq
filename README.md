# sk-zmq

Reusable ZMQ client for candle stream subscriptions.

This package provides only ZMQ communication, TTL renewal, and event listening.
It does not include any trading logic.

## Install

```bash
pip install -e /home/sanghun/workspace/sk/sk-zmq
```

## Usage

```python
from sk_zmq import ZMQClient


def on_candle_update(candle_deques):
    # Handle updated candles here
    pass


def on_critical(msg: str):
    # Handle critical alerts here
    print(msg)


client = ZMQClient(
    client_id="my-client",
    symbol="KRW-BTC",
    intervals=["minute1", "minute5"],
    candle_handler_callback=on_candle_update,
    throttle_seconds=0.1,
    zmq_gateway_host="127.0.0.1",
    zmq_gateway_req_port=5555,
    zmq_gateway_pub_port=5556,
    server_candle_ttl=300,
    candle_deque_maxlen=200,
    on_critical=on_critical,
)

client.start()
```
