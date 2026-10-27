# Changelog

## Unreleased

- `Schedule.cron('*/15 9-17 * * 1-5')`: 5-field cron schedules for Beat.
- `send(..., dedupeKey:, dedupeFor:)`: a repeat send with the same key
  within the window returns the first task's id instead of enqueuing again
  (needs a result backend; atomic on Redis).
- `Worker.stop(timeout:)`; the worker CLI takes `--drain-timeout` (30 s).
- `RedisBroker` promotes delayed messages and reaps stale ones at most once
  per `maintenanceInterval` per process (1 s, or half the visibility
  timeout), instead of on every slot's every poll.
- **RabbitMQ delays no longer block each other.** Delayed messages wait in
  `<prefix>.delay.<ms>` buckets (100 ms × 2ⁿ, each with one queue-level
  TTL) instead of one queue with per-message TTLs, where RabbitMQ only
  expired the head: a 1-hour job used to hold every shorter one behind it.
  The consumer moves a message through further buckets until it is due and
  holds the last < 100 ms itself, so nothing is handed out early. The old
  `<prefix>.scheduled` queue is still declared so its messages drain.
- **Beat failover.** A standby that becomes leader reloads every entry's
  persisted last-run time first, instead of using what it read while it
  stood by (it refired whatever the old leader had just run). Ticks no
  longer overlap. `RedisBeatLock` renews and releases with Lua
  compare-and-expire / compare-and-delete, so it never extends or deletes
  a lock another instance took over.
- **Dead-letter queue.** Messages that fail for good (after their last
  retry, unknown task names, more than `maxRequeues` `RejectTask(requeue:
  true)`s, or more than `maxRedeliveries` unacked redeliveries) are put on
  `dead_letter` with `TaskMessage.deadLetter` = `{reason, queue, at}`.
  `app.replayDeadLetters()` sends them back. `Worker`/`app.worker` take
  `deadLetterQueue` (null turns it off), `maxRequeues` (10) and
  `maxRedeliveries` (5). Messages carry a `requeues` counter.
- **Hard time limits hold the slot.** A handler past `timeLimit` is still
  recorded as timed out immediately, but its concurrency slot stays busy
  until the handler returns (with a warning every `timeLimit`), so abandoned
  handlers no longer pile up beyond `concurrency`. `ctx.isAbandoned` tells a
  handler it was abandoned. `stop()` stops waiting for them.
- Redis integration tests need `GISILA_TEST_REDIS_PORT`; they no longer
  default to a local Redis on 6379.
- **Redis password, ACL user, TLS and database.** Every Redis component
  (`RedisBroker`, `RedisResultBackend`, `RedisEventSink`/`Source`,
  `RedisControlChannel`, `RedisBeatLock`, `RedisBeatStateStore`) takes
  `options: RedisOptions(...)`, sent as `AUTH` / `SELECT` on each connection
  (TLS with an optional `SecurityContext`). `RedisOptions.parseUrl` reads
  `redis://` / `rediss://` URLs; the worker, beat, inspect and Rose CLIs take
  `--redis-url` or the `REDIS_URL` environment variable.
- **Rose dashboard is no longer open by default (breaking).** It binds
  `127.0.0.1` unless told otherwise (it was `0.0.0.0`), and `start()`
  throws when it would serve fire/retry/revoke/pause on a non-loopback
  address without `basicAuth` (`allowUnauthenticated: true` /
  `--allow-unauthenticated` to override). It sends no CORS headers (it sent
  `Access-Control-Allow-Origin: *`) and refuses POSTs that aren't
  `application/json` with `415`, so other sites can't drive it through a
  signed-in browser. Basic Auth is compared in constant time.
- **Long tasks no longer run twice.** Workers now `touch` each running
  message at a third of the broker's `visibilityTimeout`, so the Redis
  reaper only redelivers messages whose worker has actually gone away.
  New `Broker.visibilityTimeout` / `Broker.touch` (no-ops by default).
- **Per-delivery acks.** `reserve` tags each message with a fresh
  `TaskMessage.deliveryTag`; `ack` on Redis and RabbitMQ acts on that
  delivery only, so two copies of one task id (a redelivery, or a retry
  re-enqueued before the original was acked) can't clear each other.
- **No lost Redis messages.** Reserve, ETA promotion and redelivery are each
  a single Lua script, closing the crash windows between `BRPOP` and the
  ledger write and between `ZREM` and `LPUSH`. Idle workers now `BLPOP`
  per-queue notify lists (`<prefix>:notify:<queue>`) instead of the queues
  themselves; messages pushed by older producers are still picked up, after
  the reserve timeout. Undecodable payloads go to `<prefix>:dead`
  (`RedisBroker.deadLetterLimit`, `deadLetterCount()`) instead of being
  dropped. The broker targets a single Redis, not Redis Cluster.
- **Worker slots survive errors.** A failure outside the task handler
  (result backend, event sink, broker ack) is logged and the slot keeps
  consuming; the message stays unacked for redelivery.

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
