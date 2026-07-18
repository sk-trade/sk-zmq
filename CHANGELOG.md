# Changelog

All notable changes to this project are documented in this file.

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
