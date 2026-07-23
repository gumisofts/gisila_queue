import 'dart:async';

import 'package:gisila_queue/gisila_queue.dart';
import 'package:test/test.dart';

GisilaQueue _app() => GisilaQueue(
      broker: InMemoryBroker(pollInterval: const Duration(milliseconds: 5)),
      backend: InMemoryResultBackend(),
      logger: SilentQueueLogger(),
    );

void main() {
  group('time limit', () {
    test('a hard time limit fails a hung task instead of blocking forever',
        () async {
      final app = _app();
      app.task<void>(
        'hangs',
        (ctx) async => Future<void>.delayed(const Duration(seconds: 30)),
        timeLimit: const Duration(milliseconds: 100),
      );
      final worker = app.worker()..run();

      final r = await app.send<void>('hangs');
      await expectLater(
        r.get(timeout: const Duration(seconds: 2)),
        throwsA(isA<TaskFailureException>()),
      );
      expect(await r.state, TaskState.failure);

      await worker.stop();
      await app.close();
    });

    test('a task finishing within its time limit still succeeds', () async {
      final app = _app();
      app.task<int>(
        'quick',
        (ctx) async => 42,
        timeLimit: const Duration(seconds: 2),
      );
      final worker = app.worker()..run();

      final r = await app.send<int>('quick');
      expect(await r.get(timeout: const Duration(seconds: 2)), 42);

      await worker.stop();
      await app.close();
    });

    test('softTimeLimitExceeded is cooperative and does not stop the task',
        () async {
      final app = _app();
      var observedExceeded = false;
      app.task<void>(
        'polls-soft-limit',
        (ctx) async {
          await Future<void>.delayed(const Duration(milliseconds: 60));
          observedExceeded = ctx.softTimeLimitExceeded;
          return null;
        },
        softTimeLimit: const Duration(milliseconds: 30),
      );
      final worker = app.worker()..run();

      final r = await app.send<void>('polls-soft-limit');
      await r.get(timeout: const Duration(seconds: 2));
      expect(observedExceeded, isTrue);
      expect(await r.successful, isTrue);

      await worker.stop();
      await app.close();
    });
  });

  group('rate limit', () {
    test('defers excess runs instead of executing them immediately', () async {
      final app = _app();
      final runTimes = <DateTime>[];
      app.task<void>(
        'limited',
        (ctx) async => runTimes.add(DateTime.now()),
        rateLimit: RateLimit.parse('2/s'),
      );
      final worker = app.worker(concurrency: 4)..run();

      final t0 = DateTime.now();
      final results = await Future.wait(
          [for (var i = 0; i < 4; i++) app.send<void>('limited')]);
      await Future.wait([
        for (final r in results) r.get(timeout: const Duration(seconds: 3)),
      ]);

      expect(runTimes, hasLength(4));
      // At most 2 should have run within the first ~1s window.
      final withinFirstSecond =
          runTimes.where((t) => t.difference(t0) < const Duration(seconds: 1));
      expect(withinFirstSecond.length, lessThanOrEqualTo(2));

      await worker.stop();
      await app.close();
    });

    test('RateLimit.parse understands Celery-style shorthand', () {
      expect(RateLimit.parse('10/s').count, 10);
      expect(RateLimit.parse('10/s').per, const Duration(seconds: 1));
      expect(RateLimit.parse('100/m').per, const Duration(minutes: 1));
      expect(RateLimit.parse('7/h').per, const Duration(hours: 1));
      expect(() => RateLimit.parse('bogus'), throwsFormatException);
    });
  });

  group('expires', () {
    test('a task past its expiry is revoked without running', () async {
      final app = _app();
      var ran = false;
      app.task<void>('perishable', (ctx) async => ran = true);
      final worker = app.worker()..run();

      final r = await app.send<void>(
        'perishable',
        expires: DateTime.now().subtract(const Duration(seconds: 1)),
      );
      await expectLater(
        r.get(timeout: const Duration(seconds: 2)),
        throwsA(isA<TaskFailureException>()),
      );
      expect(await r.state, TaskState.revoked);
      expect(ran, isFalse);

      await worker.stop();
      await app.close();
    });

    test('a task well within its expiry runs normally', () async {
      final app = _app();
      final add = app.task<int>('add', (ctx) async => (ctx.arg(0) as int) + 1);
      final worker = app.worker()..run();

      final r = await add.applyAsync(
        args: [1],
        expiresIn: const Duration(minutes: 5),
      );
      expect(await r.get(timeout: const Duration(seconds: 2)), 2);

      await worker.stop();
      await app.close();
    });
  });
}
