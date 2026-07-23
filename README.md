# gisila_queue

A **Celery-like distributed task queue for Dart**. Define named tasks once,
enqueue them from anywhere with `.delay()` / `.applyAsync()`, and process them
with worker pools backed by a pluggable broker (Redis or in-memory) and an
optional result backend.

Part of the [Gisila](https://github.com/gumisofts/gisila) ecosystem — it
generalises the ad-hoc `BLPOP` worker loop used in `gisila-panel` into a
reusable library.

## Features

- **Named task registry** with typed producer handles (`TaskRef<T>`).
- **`.delay()` / `.applyAsync()`** producer API, including `countdown` / `eta`
  scheduling, `expires` deadlines, and per-call queue routing.
- **Pluggable broker** — `RedisBroker` for multi-process/multi-host clusters,
  `InMemoryBroker` for tests and single-process apps. The Redis broker gives
  **at-least-once delivery** (an unacked ledger + visibility-timeout reaper
  redeliver a crashed worker's in-flight messages) and **reconnects
  automatically** after a Redis restart.
- **Pluggable result backend** — `RedisResultBackend` (TTL'd) or
  `InMemoryResultBackend` (bounded via `maxEntries`/`ttl`); omit it entirely
  for fire-and-forget.
- **Retries** with exponential backoff + jitter, `ctx.retry(...)`,
  `autoRetry`, and `RejectTask` (drop or requeue).
- **Execution safety** — per-task `timeLimit` (hard) / `softTimeLimit`
  (cooperative), per-task `rateLimit` (e.g. `'10/s'`, enforced per worker
  process), and `expires` (a worker revokes rather than runs a stale task).
- **Workflow primitives** — `chain()`, `group()`/`GroupResult`, and `chord()`
  (with a backend barrier), the Dart analogues of Celery Canvas.
- **Revocation & remote control** — `AsyncResult.revoke()` plus a
  `ControlChannel` (`InMemory`/`Redis`) for pausing/resuming a worker's
  queues, the analogue of `app.control`.
- **Declarative routing** — a `Router` on `GisilaQueue` (glob or `RegExp`
  patterns), the analogue of Celery's `task_routes`.
- **`AsyncResult`** to poll state, await return values, or revoke.
- **Worker pools** with cooperative async concurrency and named-queue
  priority routing.
- **Beat scheduler** for periodic tasks (`Schedule.every`, `Schedule.dailyAt`),
  with an optional Redis leader lock + persisted last-run state so two beat
  processes don't double-fire everything.
- **CLI tooling** — `lib/src/cli` gives project `bin/worker.dart`/
  `bin/beat.dart` entrypoints shared flag parsing and graceful shutdown in a
  few lines; `bin/gisila_queue.dart inspect|purge` mirrors
  `celery inspect`/`celery purge`.
- **Pluggable `MessageCodec`** (JSON by default) with eager, producer-side
  validation — unencodable args/kwargs raise immediately at `.delay()` /
  `.applyAsync()` instead of failing deep inside a broker.
- **Optional observability** — a built-in event stream plus **GisilaRose**,
  a web dashboard for live tasks, workers, and queue depths (the analogue of
  Celery's Flower). Off by default, zero overhead until you opt in.

## Concepts (Celery ↔ gisila_queue)

| Celery                       | gisila_queue                                  |
| ----------------------------- | ---------------------------------------------- |
| `Celery()` app                | `GisilaQueue`                                  |
| `@app.task`                   | `app.task<T>(name, handler)`                   |
| `.delay()` / `.apply_async`   | `ref.delay()` / `ref.applyAsync()`             |
| broker (Redis/RabbitMQ)       | `Broker` (`RedisBroker`, in-memory)            |
| result backend                | `ResultBackend`                                |
| `AsyncResult`                 | `AsyncResult`                                  |
| `celery worker`               | `app.worker().run()` (or `WorkerCli`)          |
| `celery beat`                 | `Beat(app).run()` (or `BeatCli`)               |
| `self.retry()`                | `ctx.retry()`                                  |
| `chain`/`group`/`chord`       | `app.chain()`/`app.group()`/`app.chord()`      |
| `task.apply_async(expires=…)` | `expires` / `expiresIn`                        |
| `task_time_limit`/`soft_…`    | `Task.timeLimit` / `Task.softTimeLimit`        |
| `task_default_rate_limit`     | `Task.rateLimit` (e.g. `RateLimit.parse('10/s')`) |
| `AsyncResult.revoke()`        | `AsyncResult.revoke()`                         |
| `app.control` (pause/resume)  | `ControlChannel` (`InMemory`/`Redis`)          |
| `task_routes`                 | `Router` on `GisilaQueue`                      |
| `celery inspect`/`purge`      | `bin/gisila_queue.dart inspect|purge`          |
| `flower`                      | `GisilaRose(...).start()`                      |

## Install

```yaml
dependencies:
  gisila_queue: ^0.1.0
```

## Quick start

```dart
import 'package:gisila_queue/gisila_queue.dart';

final app = GisilaQueue(
  broker: RedisBroker(host: 'localhost', port: 6379),
  backend: RedisResultBackend(host: 'localhost', port: 6379),
);

// Register tasks (in code shared by producers and workers).
final add = app.task<int>(
  'math.add',
  (ctx) async => (ctx.arg(0) as int) + (ctx.arg(1) as int),
  maxRetries: 3,
);
```

### Producer

```dart
final r = await add.delay([2, 3]);
print(await r.get());                       // 5

// Routing + scheduling:
await add.applyAsync(
  args: [10, 20],
  queue: 'math',
  countdown: const Duration(seconds: 30),   // run ~30s from now
);
```

### Worker process

```dart
// bin/worker.dart
Future<void> main() async {
  // ...build `app` and register the same tasks...
  await app.worker(queues: ['default', 'math'], concurrency: 8).run();
}
```

Run several worker processes (or containers) to scale out; each one pulls from
the shared Redis queues. `concurrency` controls how many tasks a single
process runs at once — ideal for IO-bound work. For CPU-bound work, prefer
more processes over higher `concurrency`.

## Retries

```dart
app.task<void>(
  'emails.send',
  (ctx) async {
    try {
      await sendEmail(ctx.arg(0) as String);
    } catch (e) {
      // Re-enqueue with the task's backoff schedule, or override it:
      ctx.retry(countdown: const Duration(seconds: 10), cause: e);
    }
  },
  maxRetries: 5,
  retryBackoff: const Duration(seconds: 2), // 2s, 4s, 8s, … (+ jitter)
);
```

Set `autoRetry: true` to retry on *any* uncaught exception while attempts
remain. Throw `RejectTask(requeue: true)` to put a message back without
counting a retry, or `RejectTask()` to drop it.

## Execution safety (time limits, rate limits, expiration)

```dart
app.task<void>(
  'reports.render',
  (ctx) async {
    while (!ctx.softTimeLimitExceeded) {
      // do a chunk of work, then check again — cooperative wind-down.
    }
  },
  timeLimit: const Duration(minutes: 5),      // hard: worker abandons the wait
  softTimeLimit: const Duration(minutes: 4),  // soft: ctx.softTimeLimitExceeded
  rateLimit: RateLimit.parse('10/s'),         // per worker process
);

await add.applyAsync(args: [2, 3], expiresIn: const Duration(minutes: 1));
```

`timeLimit` cancels the worker's *await*, not the underlying computation —
Dart can't preempt a running, non-yielding handler within one isolate.
`softTimeLimit` is purely cooperative: poll `ctx.softTimeLimitExceeded` in
long-running/looping handlers and wind down early instead. A task past its
`expires` is recorded as `TaskState.revoked` instead of running.

## Workflow primitives (chain, group, chord)

```dart
// chain: sequential pipeline, each step's result feeds the next.
final result = await app.chain<String>([
  fetchUser.sig(args: [userId]),
  formatGreeting.sig(),
]).apply();

// group: run several signatures in parallel, await them all.
final results = await app.group<int>([
  resize.sig(args: [imageId, 'small']),
  resize.sig(args: [imageId, 'large']),
]).apply();
await results.get();

// chord: group + callback, firing once every member has finished.
final total = await app.chord<int>(
  [resize.sig(args: [imageId, 'small']), resize.sig(args: [imageId, 'large'])],
  sumSizes.sig(),
).apply();
```

`chord` needs a `ResultBackend` (it uses `chordArrive`/`chordAbort` as the
completion barrier) — both `InMemoryResultBackend` and `RedisResultBackend`
implement it.

## Revocation & remote control

```dart
final r = await add.delay([2, 3]);
await r.revoke(); // never runs if a worker hasn't started it yet

// From any producer process, pause/resume a worker's queue:
await app.controlChannel.publish(ControlMessage.pause('low-priority'));
await app.controlChannel.publish(ControlMessage.resume('low-priority'));
```

Pass an `InMemoryControlChannel` (single isolate) or `RedisControlChannel`
(distributed pub/sub) to `GisilaQueue(controlChannel: ...)` — the default
`NoopControlChannel` makes both calls no-ops. Revocation is cooperative
"don't start it", not "kill it mid-flight" (see `TaskContext.isRevoked` for
checking inside a running handler).

## Declarative routing

```dart
final router = Router()
  ..route('emails.*', queue: 'emails')
  ..routeMatching(RegExp(r'^reports\..*'), queue: 'reports');

final app = GisilaQueue(broker: RedisBroker(), router: router);
```

Queue resolution order for a call: an explicit `queue:` always wins, then
`router`, then the task's own registered default queue, then
`GisilaQueue.defaultQueue`.

## CLI tooling

```dart
// bin/worker.dart
Future<void> main(List<String> argv) async {
  final cli = WorkerCli.parse(argv);
  if (cli.help) return print(cli.usage);
  final app = cli.buildApp();
  registerMyTasks(app);
  await cli.run(app); // graceful SIGINT/SIGTERM shutdown included
}
```

```sh
dart run bin/worker.dart --queues=default,emails --concurrency=8
dart run bin/beat.dart --lock-ttl=30
dart run gisila_queue:gisila_queue inspect --queues=default
dart run gisila_queue:gisila_queue purge --queue=default
```

`WorkerCli`/`BeatCli` (`package:gisila_queue/gisila_queue_cli.dart`) parse the
standard `--redis-host/port`, `--prefix`, `--loglevel` flags plus their own
(`--queues`/`--concurrency` for workers; `--tick-interval`/`--lock-ttl` for
beat, wiring a `RedisBeatLock` + `RedisBeatStateStore` automatically) and
install SIGINT/SIGTERM shutdown hooks that close every connection cleanly.

## Periodic tasks (beat)

```dart
final beat = Beat(
  app,
  lock: RedisBeatLock(host: 'localhost'),           // only one active leader
  stateStore: RedisBeatStateStore(host: 'localhost'), // survives a restart
)
  ..schedule('nightly-report', 'reports.nightly',
      Schedule.dailyAt(2, minute: 30))
  ..schedule('heartbeat', 'health.ping',
      Schedule.every(const Duration(minutes: 1)));

await beat.run(); // safe to start more than one process with a lock set
```

Without `lock`/`stateStore` (the default), `Beat` behaves as before: assume a
single instance, and a restart forgets `lastRun` for every entry.

## Observability (GisilaRose)

Like Flower for Celery, **GisilaRose** is an *optional* dashboard that shows
live tasks, worker status, queue depths, and per-task-type stats. It's driven
by a lightweight event stream that producers and workers emit — which is
**off by default**: a `GisilaQueue` with no `events:` sink pays nothing.

Turn it on by giving the app an event sink. Use `RedisEventSink` so a separate
monitor process (across hosts) can read the stream:

```dart
// Producer & worker processes — same `events:` sink everywhere.
final app = GisilaQueue(
  broker: RedisBroker(host: 'localhost'),
  backend: RedisResultBackend(host: 'localhost'),
  events: RedisEventSink(host: 'localhost'),   // ← opt in
);
```

Then run the dashboard. The simplest way is the bundled binary (the analogue
of `celery flower`):

```sh
dart run gisila_queue:rose --redis-host=localhost --port=5555
# → GisilaRose on http://localhost:5555
```

…or embed it in your own process:

```dart
import 'package:gisila_queue/gisila_rose.dart';

final rose = GisilaRose(
  source: RedisEventSource(host: 'localhost'),
  broker: RedisBroker(host: 'localhost'),  // optional: enables queue-depth panel
  port: 5555,
);
await rose.start();
```

It serves a self-contained web UI at `/` plus a JSON API:

| Endpoint              | Returns                                          |
| --------------------- | ------------------------------------------------ |
| `GET /api/stats`      | totals, worker/queue rollups, per-task-type stats|
| `GET /api/workers`    | each worker's status + counters                  |
| `GET /api/tasks`      | recent tasks (`?state=&name=&queue=&limit=&offset=`) |
| `GET /api/tasks/<id>` | one task's full detail                           |
| `GET /api/queues`     | broker depth per queue                           |
| `GET /api/events`     | Server-Sent Events firehose (live tail)          |

For a **single-isolate** app or tests, skip Redis: share one
`InMemoryEventBus` as both the app's `events:` sink and the rose's `source:`.

The events themselves are also usable directly — attach a `RedisEventSink`,
`InMemoryEventBus`, or your own `QueueEventSink` to forward task lifecycle
events anywhere (metrics, audit logs, alerting).

## In-memory mode (no Redis)

Swap the broker/backend for the in-memory implementations to run producers
and a worker in a single isolate — perfect for tests and local dev:

```dart
final app = GisilaQueue(
  broker: InMemoryBroker(),
  backend: InMemoryResultBackend(),
);
```

See [`example/main.dart`](example/main.dart) for a complete self-contained run.

## Architecture

```
producer ──ref.delay()──▶ GisilaQueue.send ──▶ Broker.enqueue
                                                   │  (Redis list / ZSET for ETA)
                                                   ▼
                              Worker.reserve ◀── Broker (BRPOPLPUSH into an
                                   │              unacked ledger + ETA promotion;
                                   │              a reaper redelivers anything past
                                   │              its visibility timeout)
                                   ▼
                              task handler ──▶ ResultBackend.store(state, result)
                                   ▲                                    │
                                   └────── retry (backoff, re-enqueue)  │
                                                                        ▼
producer ◀──── AsyncResult.get() ◀───────────── ResultBackend.fetch
```

## License

BSD 3-Clause — see [LICENSE](LICENSE).
