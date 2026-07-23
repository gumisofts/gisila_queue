// ignore_for_file: avoid_print
//
// A self-contained example that runs entirely in one process using the
// in-memory broker/backend — no Redis required. Swap the broker/backend for
// `RedisBroker` / `RedisResultBackend` to go distributed across processes.
import 'package:gisila_queue/gisila_queue.dart';

Future<void> main() async {
  final app = GisilaQueue(
    broker: InMemoryBroker(),
    backend: InMemoryResultBackend(),
    logger: ConsoleQueueLogger(minLevel: LogLevel.info),
  );

  // Register tasks. The generic argument is the result type.
  final add = app.task<int>(
    'math.add',
    (ctx) async => (ctx.arg(0) as int) + (ctx.arg(1) as int),
  );

  var flaky = 0;
  app.task<String>(
    'demo.flaky',
    (ctx) async {
      flaky++;
      if (flaky < 3) ctx.retry(countdown: const Duration(milliseconds: 50));
      return 'succeeded on attempt ${ctx.retries + 1}';
    },
    maxRetries: 5,
    retryJitter: false,
  );

  // Start a worker pool in the background.
  final worker = app.worker(concurrency: 4);
  unawaited(worker.run());

  // ── Producer side ──
  final r = await add.delay([2, 3]);
  print('2 + 3 = ${await r.get()}');

  final delayed = await add.applyAsync(
    args: [10, 20],
    countdown: const Duration(milliseconds: 200),
  );
  print('delayed 10 + 20 = ${await delayed.get()}');

  final flakyResult = await app.send<String>('demo.flaky', maxRetries: 5);
  print('flaky => ${await flakyResult.get()}');

  await worker.stop();
  await app.close();
}

void unawaited(Future<void> f) {}
