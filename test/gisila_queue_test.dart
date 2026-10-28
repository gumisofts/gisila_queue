import 'dart:async';

import 'package:gisila_queue/gisila_queue.dart';
import 'package:test/test.dart';

GisilaQueue _app() => GisilaQueue(
      broker: InMemoryBroker(pollInterval: const Duration(milliseconds: 5)),
      backend: InMemoryResultBackend(),
      logger: SilentQueueLogger(),
    );

void main() {
  group('message', () {
    test('round-trips through encode/decode', () {
      final msg = TaskMessage(
        id: 'abc',
        task: 'math.add',
        queue: 'default',
        args: [1, 2],
        kwargs: {'k': 'v'},
        maxRetries: 3,
        eta: DateTime.parse('2026-01-01T00:00:00Z'),
      );
      final back = TaskMessage.decode(msg.encode());
      expect(back.id, 'abc');
      expect(back.task, 'math.add');
      expect(back.args, [1, 2]);
      expect(back.kwargs, {'k': 'v'});
      expect(back.maxRetries, 3);
      expect(back.eta!.toUtc(), DateTime.parse('2026-01-01T00:00:00Z'));
    });
  });

  group('end to end', () {
    test('runs a task and returns its result', () async {
      final app = _app();
      final add = app.task<int>(
        'math.add',
        (ctx) async => (ctx.arg(0) as int) + (ctx.arg(1) as int),
      );
      final worker = app.worker()..run();

      final r = await add.delay([2, 3]);
      expect(await r.get(timeout: const Duration(seconds: 2)), 5);
      expect(await r.successful, isTrue);

      await worker.stop();
    });

    test('records failure and rethrows on get', () async {
      final app = _app();
      app.task<void>('boom', (ctx) async => throw StateError('nope'));
      final worker = app.worker()..run();

      final r = await app.send<void>('boom');
      await expectLater(
        r.get(timeout: const Duration(seconds: 2)),
        throwsA(isA<TaskFailureException>()),
      );
      expect(await r.state, TaskState.failure);

      await worker.stop();
    });

    test('retries until success', () async {
      final app = _app();
      var attempts = 0;
      app.task<String>(
        'flaky',
        (ctx) async {
          attempts++;
          if (attempts < 3) {
            ctx.retry(countdown: const Duration(milliseconds: 10));
          }
          return 'ok@${ctx.retries}';
        },
        maxRetries: 5,
        retryJitter: false,
      );
      final worker = app.worker()..run();

      final r = await app.send<String>('flaky', maxRetries: 5);
      expect(await r.get(timeout: const Duration(seconds: 3)), 'ok@2');
      expect(attempts, 3);

      await worker.stop();
    });

    test('gives up after maxRetries and marks failure', () async {
      final app = _app();
      var attempts = 0;
      app.task<void>(
        'always-fails',
        (ctx) async {
          attempts++;
          throw StateError('boom');
        },
        maxRetries: 2,
        autoRetry: true,
        retryBackoff: const Duration(milliseconds: 5),
        retryJitter: false,
      );
      final worker = app.worker()..run();

      final r = await app.send<void>('always-fails', maxRetries: 2);
      await expectLater(
        r.get(timeout: const Duration(seconds: 3)),
        throwsA(isA<TaskFailureException>()),
      );
      expect(attempts, 3); // initial + 2 retries
      await worker.stop();
    });

    test('honours countdown scheduling', () async {
      final app = _app();
      DateTime? ranAt;
      app.task<void>('later', (ctx) async => ranAt = DateTime.now());
      final worker = app.worker()..run();

      final enqueuedAt = DateTime.now();
      final r = await app.send<void>(
        'later',
        eta: enqueuedAt.add(const Duration(milliseconds: 300)),
      );
      await r.get(timeout: const Duration(seconds: 2));
      expect(ranAt!.difference(enqueuedAt).inMilliseconds,
          greaterThanOrEqualTo(250));
      await worker.stop();
    });

    test('unknown task is recorded as failure once requeues run out', () async {
      final app = _app();
      final worker = app.worker(
          maxRequeues: 2, unknownTaskBackoff: const Duration(milliseconds: 1))
        ..run();
      final r = await app.send<void>('does-not-exist');
      await expectLater(
        r.get(timeout: const Duration(seconds: 2)),
        throwsA(isA<TaskFailureException>()),
      );
      await worker.stop();
    });
  });

  group('routing', () {
    test('worker only consumes its queues', () async {
      final app = _app();
      app.task<int>('echo', (ctx) async => ctx.arg(0) as int, queue: 'high');
      // Worker only listens on 'low'.
      final worker = app.worker(queues: ['low'])..run();

      final r = await app.send<int>('echo', args: [7], queue: 'high');
      // Not consumed -> stays pending.
      await Future<void>.delayed(const Duration(milliseconds: 200));
      expect(await r.state, TaskState.pending);
      expect(await app.broker.length('high'), 1);

      await worker.stop();
    });
  });

  group('backoff', () {
    test('is exponential and capped', () {
      final t = Task(
        name: 't',
        handler: (_) async => null,
        retryBackoff: const Duration(seconds: 1),
        retryBackoffMax: const Duration(seconds: 10),
        retryJitter: false,
      );
      expect(t.backoffFor(1), const Duration(seconds: 1));
      expect(t.backoffFor(2), const Duration(seconds: 2));
      expect(t.backoffFor(3), const Duration(seconds: 4));
      expect(t.backoffFor(4), const Duration(seconds: 8));
      expect(t.backoffFor(5), const Duration(seconds: 10)); // capped
    });
  });

  group('beat', () {
    test('fires due entries on tick', () async {
      final app = _app();
      var runs = 0;
      app.task<void>('tick-task', (ctx) async => runs++);
      final worker = app.worker()..run();

      final beat = Beat(app)
        ..schedule('every-10ms', 'tick-task',
            Schedule.every(const Duration(milliseconds: 10)));

      final t0 = DateTime.parse('2026-01-01T00:00:00Z');
      await beat.tick(t0);
      await beat.tick(t0.add(const Duration(milliseconds: 5))); // not due
      await beat.tick(t0.add(const Duration(milliseconds: 20))); // due

      await Future<void>.delayed(const Duration(milliseconds: 200));
      expect(runs, 2);
      await worker.stop();
    });
  });
}
