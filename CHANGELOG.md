# Changelog

## Unreleased

- **`RabbitBroker`** — RabbitMQ (AMQP) broker via `dart_amqp`. Durable queues,
  manual ack for at-least-once delivery, and ETA/countdown via per-message TTL
  + dead-letter exchange (stock RabbitMQ, no delayed-message plugin). Tagged
  integration tests (`rabbitmq`) and `--broker=rabbitmq` on the throughput
  benchmark.
- **Observability layer (opt-in).** Producers and workers now emit lifecycle
  `QueueEvent`s to an optional `QueueEventSink` on `GisilaQueue` (defaults to a
  zero-overhead `NoopEventSink`):
  - Sinks/sources: `InMemoryEventBus` (single isolate), `RedisEventSink` /
    `RedisEventSource` (pub/sub + capped history), `MultiEventSink`.
  - Workers report a stable id, periodic heartbeats, and per-outcome events.
- **GisilaRose** — an optional web dashboard (`package:gisila_queue/gisila_rose.dart`)
  for live tasks, workers, and queue depths, with a JSON API and SSE live tail.
  Run standalone via `dart run gisila_queue:rose`. The analogue of Celery's
  Flower.
  - Light-theme UI with timestamps, task detail drawer, and per-worker drill-down.
  - Flower-style actions: fire, retry (new id), revoke, queue pause/resume.
  - Optional HTTP Basic Auth (`--basic-auth=user:pass`).
  - `GET /api/workers/<id>`, `worker=` task filter, `active_task_ids` on workers.

## 0.1.0

Initial release.

- `GisilaQueue` app with a named-task registry and typed `TaskRef` producers
  (`.delay()` / `.applyAsync()`).
- Pluggable `Broker`: `InMemoryBroker` (single-isolate / tests) and
  `RedisBroker` (distributed, list-based queues + sorted-set ETA scheduling).
- Pluggable `ResultBackend`: `InMemoryResultBackend` and `RedisResultBackend`
  (TTL'd result keys); backend is optional for fire-and-forget use.
- `Worker` with cooperative async concurrency, named-queue priority routing,
  retries with exponential backoff + jitter, `ctx.retry()` / `RejectTask`,
  and success/failure/retry observer hooks.
- `AsyncResult` for polling task state and awaiting return values.
- `Beat` scheduler for periodic tasks (`Schedule.every`, `Schedule.dailyAt`).
