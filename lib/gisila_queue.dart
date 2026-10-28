/// gisila_queue — a Celery-like distributed task queue for Dart.
///
/// Define named tasks, enqueue them from anywhere with `.delay()` /
/// `.applyAsync()`, and process them with worker pools backed by a pluggable
/// [Broker] (Redis, RabbitMQ, or in-memory) and optional [ResultBackend]. Supports
/// retries with exponential backoff, ETA / countdown scheduling, named
/// queues, and a [Beat] scheduler for periodic tasks.
///
/// ```dart
/// import 'package:gisila_queue/gisila_queue.dart';
///
/// final app = GisilaQueue(
///   broker: RedisBroker(host: 'localhost'),
///   backend: RedisResultBackend(host: 'localhost'),
/// );
///
/// final add = app.task<int>('math.add', (ctx) async =>
///     (ctx.arg(0) as int) + (ctx.arg(1) as int), maxRetries: 3);
///
/// // ── producer ──
/// final r = await add.delay([2, 3]);
/// print(await r.get()); // 5
///
/// // ── worker process ──
/// await app.worker(concurrency: 4).run();
/// ```
library;

export 'src/app.dart';
export 'src/backend/in_memory_backend.dart';
export 'src/backend/redis_backend.dart';
export 'src/backend/result_backend.dart';
export 'src/beat/beat.dart';
export 'src/beat/coordination.dart';
export 'src/beat/schedule.dart';
export 'src/broker/broker.dart';
export 'src/broker/in_memory_broker.dart';
export 'src/broker/rabbit_broker.dart';
export 'src/broker/redis_broker.dart';
export 'src/canvas.dart';
export 'src/codec.dart';
export 'src/control.dart';
export 'src/events/event.dart';
export 'src/events/event_sink.dart';
export 'src/events/in_memory_event_bus.dart';
export 'src/events/redis_event_sink.dart';
export 'src/exceptions.dart';
export 'src/logging.dart';
export 'src/message.dart';
export 'src/namespace.dart';
export 'src/rate_limit.dart';
export 'src/redis_options.dart';
export 'src/result.dart';
export 'src/router.dart';
export 'src/task.dart';
export 'src/task_state.dart';
export 'src/worker/worker.dart';
