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
    # Treat as read-only unless using deep_copy
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
    callback_snapshot_mode="live",  # "live", "deque_copy", "deep_copy"
)

client.start()
```

## Callback snapshot modes

The callback now runs outside the internal storage lock.
Depending on your needs, choose how data is passed into the callback:

- live: Passes the live deque dictionary. Highest performance, highest risk; values can change during the callback.
- deque_copy: Copies each deque container (elements are shared). Stable container, shared elements.
- deep_copy: Copies deque containers and each candle dict. Most stable, most overhead.

Treat the callback payload as read-only unless using deep_copy.

```python
# Snapshot mode examples
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
    callback_snapshot_mode="deque_copy",
)

# concurrency-safe access pattern (inside callback)

def on_candle_update(candle_deques):
    latest = {k: v[-1] if v else None for k, v in candle_deques.items()}
```
