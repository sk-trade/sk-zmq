# sk-zmq

Reusable ZMQ client for aggregated candle stream subscriptions (version 0.2.1).

This package provides only ZMQ communication, TTL renewal, and event listening.
It does not include any trading logic.

## Install

```bash
pip install -e .
```

For development checks:

```bash
uv run --group dev pytest -q
uv build
```

## Usage

A compatible candle gateway must already be running at the configured host and
ports. `start()` returns `False` if an initial snapshot cannot be loaded or the
local SUB listener cannot be initialized.

Supported aggregated intervals: `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, `4h`.
`10m`, `1d`, and native `1s` streams are not accepted by this client.
Construction fails immediately if `intervals` is empty or
`candle_deque_maxlen` is not greater than zero.
`candle_deque_maxlen` may exceed 200; initial gateway history is capped at 200
while the local deque continues growing up to the configured maximum.

```python
import time

from sk_zmq import ZMQClient


def on_candle_update(candle_deques):
    latest = {
        interval: candles[-1] if candles else None
        for interval, candles in candle_deques.items()
    }
    print(latest)


def on_critical(msg: str):
    # Handle critical alerts here
    print(msg)


client = ZMQClient(
    client_id="my-client",
    symbol="KRW-BTC",
    intervals=["1m", "5m", "15m"],
    candle_handler_callback=on_candle_update,
    throttle_seconds=0.1,
    zmq_gateway_host="127.0.0.1",
    zmq_gateway_req_port=11556,
    zmq_gateway_pub_port=11558,
    server_candle_ttl=300,
    candle_deque_maxlen=200,
    on_critical=on_critical,
    callback_snapshot_mode="deque_copy",  # "live", "deque_copy", "deep_copy"
    exchange="upbit",
)

try:
    if not client.start():
        raise RuntimeError(
            "Could not start the ZMQ client. Check the gateway request/PUB "
            "addresses, ports, exchange, symbol, and intervals."
        )
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    client.stop()
```

## Exchange and payload expectations

`exchange` selects the candle topic prefix used by the client.

For example, `exchange="upbit"` subscribes to:

`UPBIT:CANDLE:{symbol}:{interval}:`

Use `exchange="binance"` with a canonical symbol such as `USDT-BTC` for Binance.
The broadcaster runs both providers and routes requests using `exchange`.
`ZMQClient` consumes the aggregated `subscribe_candle` product, with its fixed
candle topic prefix. It does not subscribe to normalized raw/native streams or
provide HTTP REST wrappers. Raw stream consumers must use the exact `topic`
returned by `subscribe`; orderbook options identify separate leases and topics.
REST daily/week/month candles remain separate provider-native capabilities.

The gateway publishes `UPDATE`, `CLOSE`, and `RECONCILE` envelopes with `candle`
(and `new` for CLOSE). Candle `ts` is the bucket open time in epoch milliseconds.
Every event, including `RESYNC_REQUIRED`, carries `stream_id` (provider runtime
identity) and `seq` (monotonically increasing provider publication counter).
Candle rows are upserted by `ts` and kept sorted; CLOSE does not duplicate a bucket
or replace a different newer bucket. Freshness is determined by sequence, so later
same-bucket events can change price at equal volume or reduce volume. RECONCILE
replaces a stored timestamp; a missing timestamp is ignored. Unknown, malformed,
unsequenced, duplicate and stale events are ignored.

## Startup, recovery, and leases

`start()` prepares and runs the SUB listener before requesting snapshots for all
intervals. Successful `subscribe_candle` responses must carry `stream_id` and
`snapshot_seq`, captured atomically with the candle data by the gateway. The client
installs that history and boundary, then replays only matching-runtime buffered
events with `seq > snapshot_seq`, in ascending sequence order. Same-runtime
sequences already applied or covered by the snapshot are discarded. Counters are
provider-wide, so sequence gaps between this client's intervals are normal.
Snapshot history is sorted and deduplicated. Callback and
renewal workers begin only after startup succeeds. A failed startup stops the
listener and leaves no partial history; call `stop()` in `finally` to release
leases and the context, as in the example above.

An interval's fresh `RESYNC_REQUIRED` event or a SUB transport disconnect pauses
callback delivery while live events are buffered. A different `stream_id` in live
events or renewal responses requires fresh snapshots for all intervals; counters
from separate runtimes are never compared. Events from a replaced runtime are
ignored. A restart discovered during an interval recovery expands recovery to all
intervals before publishing any new state. After reconnect, the client
reloads snapshots and merges buffered events before resuming callbacks. Recovery
requests use the same exchange, symbol, and interval as the original lease.
Failed recovery requests retain stored data and retry; `on_critical` reports
failed recovery attempts. Storage read directly during recovery is the last
known state. Each interval buffers at most 10,000 events; overflow invalidates
the in-flight snapshot and requires another snapshot instead of silently
accepting incomplete history. A further recovery notice also invalidates an
in-flight snapshot when it is not covered by the snapshot boundary. Missing
ordering metadata fails snapshot loading; there is no compatibility fallback to
volume or timestamp freshness. A same-runtime snapshot older than already applied
sequence state is rejected.

Renewal uses `subscribe_candle` with `history_count=1` every half TTL and keeps
existing stored history. Set `server_candle_ttl` to the server's lease TTL.
`stop()` waits for in-flight subscription requests before unsubscribing each
unique interval, joins workers, and terminates the ZMQ context. Repeated calls
are safe. The broadcaster uses shared leases, so clients sharing the same
exchange/symbol/interval must coordinate explicit unsubscription.

PUB/SUB provides no durable replay or per-consumer acknowledgement. This client
recovers from transport disconnects, runtime changes and explicit recovery notices.
The sequence contract orders equal-volume updates and snapshot handoff, but does
not prove delivery of every publication or provide durable replay. RECONCILE and
recovery snapshots supply authoritative corrections.

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
    intervals=["1m", "5m", "15m"],
    candle_handler_callback=on_candle_update,
    throttle_seconds=0.1,
    zmq_gateway_host="127.0.0.1",
    zmq_gateway_req_port=11556,
    zmq_gateway_pub_port=11558,
    server_candle_ttl=300,
    candle_deque_maxlen=200,
    on_critical=on_critical,
    callback_snapshot_mode="deque_copy",
    exchange="upbit",
)

# concurrency-safe access pattern (inside callback)

def on_candle_update(candle_deques):
    latest = {k: v[-1] if v else None for k, v in candle_deques.items()}
```
