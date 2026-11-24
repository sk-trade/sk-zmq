# sk-zmq

`sk-zmq` is a threaded ZeroMQ client for ordered, recoverable candle streams.
It owns gateway requests, subscription renewal, live event ingestion, snapshot
recovery, bounded in-memory candle storage, and callback delivery. It does not
contain trading strategy, order, or account logic.

Python 3.10 or newer is required.

## Install

From a checkout:

```bash
python -m pip install .
```

For development:

```bash
uv sync --group dev
```

## Use the client

A compatible gateway must already expose the request and publish ports.
`start()` returns `False` when the subscriber or initial snapshots cannot be
prepared. The context-manager form raises `RuntimeError` instead.

```python
import time

from sk_zmq import ZMQClient


def handle_candles(candles_by_interval):
    latest_by_interval = {
        interval: candles[-1] if candles else None
        for interval, candles in candles_by_interval.items()
    }
    print(latest_by_interval)


client = ZMQClient(
    client_id="analytics-worker",
    symbol="KRW-BTC",
    intervals=["1m", "5m", "15m"],
    candle_handler_callback=handle_candles,
    exchange="upbit",
    zmq_gateway_host="127.0.0.1",
    zmq_gateway_req_port=11556,
    zmq_gateway_pub_port=11558,
    server_candle_ttl=300,
    candle_deque_maxlen=500,
    callback_snapshot_mode="deque_copy",
)

try:
    if not client.start():
        raise RuntimeError("Could not initialize the candle stream")
    while client.is_running:
        time.sleep(1)
finally:
    client.stop()
```

`stop()` is idempotent and waits for owned workers and in-flight subscription
requests when called from outside the client's workers. Always call it in
`finally`, or use the client as a context manager.

`is_running` checks lifecycle state and worker liveness, not stream freshness
or gateway availability. For explicit shutdown confirmation, inspect
`client.state is ClientState.STOPPED` (import `ClientState` from `sk_zmq`).

### Upgrading from 0.2.1

The constructor, callbacks, supported intervals, and gateway wire format remain
compatible. Internal worker threads and synchronization objects are no longer
public; replace `client.threads` liveness checks with `client.is_running` and
rely on external `stop()` for worker cleanup. See [CHANGELOG.md](CHANGELOG.md)
for behavior changes.

## Configuration contract

Supported aggregated intervals are `1m`, `3m`, `5m`, `15m`, `30m`, `1h`, and
`4h`. Empty or unsupported interval lists are rejected before a ZMQ context is
allocated. Client identifiers, symbols, exchange names, and hosts must be
non-empty. Ports must be in the TCP port range; TTL and deque capacity must be
positive; throttle values must be finite and non-negative.

The initial gateway history request is capped at 200 rows. A larger
`candle_deque_maxlen` still allows the local deque to grow to its configured
capacity as live events arrive.

## Stream contract

For `exchange="upbit"`, the client subscribes to topics with this prefix:

```text
UPBIT:CANDLE:{symbol}:{interval}:
```

The gateway may publish `UPDATE`, `CLOSE`, `RECONCILE`, and `RESYNC_REQUIRED`.
Every publication must include a non-empty `stream_id` and a non-negative integer
`seq`. Successful snapshot responses must include the same runtime identity and
`snapshot_seq` boundary.

The client applies these rules:

- `seq` is authoritative within one `stream_id`; duplicate and stale events are
  ignored.
- Counters from different runtimes are never compared.
- Candle rows are upserted by the bucket-open timestamp `ts` and remain sorted.
- `RECONCILE` only replaces a timestamp already present in storage.
- Malformed topics, frames, payloads, candles, and ordering metadata are ignored.
- A `CLOSE` event must contain a later `new` candle.

See [ARCHITECTURE.md](ARCHITECTURE.md) for ownership and recovery invariants.

## Startup and recovery

The SUB listener starts before snapshot requests, so publications that race with
startup are buffered. Snapshots for a recovery set are validated before any of
them become visible. Matching buffered events above each `snapshot_seq` are then
replayed in sequence order.

A transport reconnect, a fresh `RESYNC_REQUIRED`, a new runtime identity, or a
renewal response from a different runtime triggers recovery. Callback delivery
pauses while affected events are buffered. A runtime change expands recovery to
all intervals. Each interval can buffer at most 10,000 events; overflow
invalidates the in-flight snapshot and forces another recovery attempt rather
than accepting incomplete data.

Failed recovery preserves the last committed candles and retries. Configure
`on_critical` to receive recovery failures and repeated renewal failures.

PUB/SUB does not provide durable replay or per-consumer acknowledgement. This
client restores a consistent current snapshot after recognized disconnects and
runtime changes; it does not prove receipt of every publication.

## Reading candle data

Callbacks run without the internal data lock held. Choose the callback payload
mode according to the mutability and performance trade-off:

- `live`: the internal mapping and deques; fastest, but they can change while the
  callback runs.
- `deque_copy`: copied deque containers with shared candle dictionaries.
- `deep_copy`: copied deques and candle dictionaries.

Treat callback values as read-only. Outside callbacks, prefer the public snapshot
method over direct access to the backward-compatible `candle_deques` property:

```python
snapshot = client.get_candle_snapshot()  # deep_copy by default
latest_5m = snapshot["5m"][-1] if snapshot["5m"] else None
```

## Development

The repository keeps one locked development environment and one CI command set:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest --cov=sk_zmq --cov-report=term-missing -q
uv build
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the contribution contract.
