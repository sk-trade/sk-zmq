# Changelog

All notable changes to this project are documented in this file.

## [0.2.1] - 2026-10-14

### Fixed

- Implement the final aggregated candle stream_id/seq/snapshot_seq contract:
  replay only matching-runtime events beyond the atomic snapshot boundary, in
  sequence order, and discard stale or duplicate publications.
- Use sequence freshness instead of volume, including equal-volume price changes
  and legitimate volume reductions. Upsert candle rows by timestamp.
- Recover all intervals after a runtime change in events, snapshots or renewal;
  never compare counters across runtimes or accept events from a retired runtime.
- Reject missing ordering metadata, stale snapshots and recovery notices beyond
  snapshot coverage. Preserve bounded overflow recovery, TTL renewal and cleanup.
- Add core sequence/restart tests, including actual TCP runtime restart recovery.

## [0.2.0] - 2026-10-14

### Changed

- Support aggregated intervals 1m, 3m, 5m, 15m, 30m, 1h, 4h; remove legacy 10m/1d.
- Start SUB reception before snapshots and merge buffered candle events by timestamp.
- Recover from RESYNC_REQUIRED and transport reconnects using fresh snapshots;
  retry failed or invalidated snapshots with bounded event buffering.
- Ignore late UPDATE/CLOSE buckets and decreasing same-bucket volume while
  preserving authoritative RECONCILE corrections, callback modes, TTL renewal,
  and serialized cleanup.
- Consolidate tests around candle integrity, real socket handoff/reconnection,
  callbacks, renewal and shutdown races; remove redundant declaration/copy/log checks.

## [0.1.5] - 2026-08-15

### Changed

- Simplified listener, renewal, callback, and shutdown lifecycle handling while preserving the public client contract.
- Allow local candle deques larger than 200 while limiting initial gateway history requests to the broadcaster's 200-candle maximum.

### Fixed

- Reject empty interval lists and non-positive candle deque capacities before allocating ZMQ resources.

## [0.1.4] - 2026-07-19

### Changed

- Centralized gateway response handling around explicit result codes and validated response shapes.
- Made client startup, callback-driven shutdown, and repeated shutdown follow serialized lifecycle transitions.
- Updated the README lifecycle examples and startup-failure guidance.

### Fixed

- Ignore malformed gateway responses, candle payloads, topic segments, and invalid event values without corrupting client state.
- Apply initial snapshots transactionally and reject malformed or failed snapshots before worker threads are published.
- Trigger callbacks only for material candle changes, including no-op update, close, reconcile, and zero-capacity cases.
- Renew short-lived subscriptions before expiry and cancel retry backoff promptly during shutdown.
- Bound unsubscribe requests, preserve renewal-before-unsubscribe ordering, and complete retryable cleanup after failures.
- Fail startup when listener or worker initialization fails while keeping successful shutdown idempotent and race-safe.
