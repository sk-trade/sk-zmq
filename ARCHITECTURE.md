# Architecture

`sk-zmq` is a small modular package around one public orchestration class,
`ZMQClient`.

## Module map

- `sk_zmq.client` owns lifecycle transitions, worker threads, callback delivery,
  and coordination between transport and recovery.
- `sk_zmq._config` validates and normalizes constructor input before external
  resources are allocated.
- `sk_zmq._gateway` owns synchronous REQ/REP serialization, timeout, retry, and
  response-shape handling.
- `sk_zmq._protocol` owns event types and pure validation/parsing for topics,
  candles, ordering metadata, snapshots, and gateway failures.
- `sk_zmq._store` owns candle deques, ordering watermarks, runtime retirement,
  recovery buffers, and transactional snapshot commits under one data lock.

The leading underscore modules are implementation details. The supported import
surface is `ZMQClient` and `ClientState` from `sk_zmq`.

## State ownership

`ZMQClient` owns exactly one ZMQ context. The request lock serializes subscribe,
renew, recovery, and unsubscribe traffic over short-lived REQ sockets. The SUB
socket is created and closed by the listener thread only.

`CandleStore` owns the data lock protecting candle deques, per-interval ordering
watermarks, buffered events, runtime retirement, and recovery-generation state.
`ZMQClient` consumes store outcomes and publishes thread events. User callbacks
are never invoked while the store lock is held.

The lifecycle lock serializes `start()` and `stop()` state publication. A caller
inside a worker may initiate shutdown without joining itself; a finalizer marks
the client stopped after that worker exits.

## Ordering and recovery invariants

1. A sequence is comparable only inside the same `stream_id`.
2. A snapshot can replace committed state only when all requested intervals have
   valid ordering metadata and one compatible runtime.
3. Events at or below `snapshot_seq` are covered by the snapshot and are not
   replayed.
4. At most one event for a given sequence advances the committed watermark.
5. A runtime change retires the previous runtime after a new snapshot commits.
6. Recovery failure never clears the last committed candle set.
7. Callback delivery remains paused while any interval has an active recovery
   buffer.
8. Shutdown waits for request serialization before sending unsubscribe requests.

## Change guidance

Keep protocol validation pure and add malformed-input cases in
`tests/test_protocol.py`. Put ordering and recovery regressions in
`tests/test_events.py`, lifecycle races in `tests/test_lifecycle.py`, and actual
socket handoff behavior in `tests/test_integration.py`.

Do not add a compatibility fallback that infers freshness from timestamp, price,
or volume. Extend the gateway ordering contract explicitly instead.
