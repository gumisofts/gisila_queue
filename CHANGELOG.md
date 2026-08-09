# Changelog

## Unreleased

## 0.1.1

- **`RabbitBroker`** — RabbitMQ (AMQP) broker via `dart_amqp`. Durable queues,
  manual ack for at-least-once delivery, and ETA/countdown via per-message TTL
  + dead-letter exchange (stock RabbitMQ, no delayed-message plugin). Tagged
  integration tests (`rabbitmq`) and `--broker=rabbitmq` on the throughput
  benchmark.
- **GisilaRose** dashboard polish: light-theme UI with timestamps, task detail
  drawer, per-worker drill-down, Flower-style fire/retry/revoke and queue
  pause/resume, optional HTTP Basic Auth, `GET /api/workers/<id>`, `worker=`
  task filter, and `active_task_ids` on workers.
- Fix worker `timeLimit` abandon path: avoid `Future.catchError((_) => null)`,
  which typed handlers (e.g. `Future<bool>`) reject as an invalid null error
  replacement and crash the worker.

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
