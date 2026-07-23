import 'dart:async';

import 'package:gisila_queue/gisila_queue.dart';
import 'package:test/test.dart';

GisilaQueue _app({ControlChannel? controlChannel}) => GisilaQueue(
      broker: InMemoryBroker(pollInterval: const Duration(milliseconds: 5)),
      backend: InMemoryResultBackend(),
      logger: SilentQueueLogger(),
      controlChannel: controlChannel ?? InMemoryControlChannel(),
    );

void main() {
  group('AsyncResult.revoke', () {
    test('a task revoked before a worker starts is never run', () async {
      final app = _app();
      var ran = false;
      app.task<void>('never_runs', (ctx) async => ran = true);

      final r = await app.send<void>('never_runs');
      await r.revoke();
      expect(await r.state, TaskState.revoked);

      final worker = app.worker()..run();
      // Give the worker a moment to have picked it up if it were going to.
      await Future<void>.delayed(const Duration(milliseconds: 200));
      expect(ran, isFalse);
      expect(await r.state, TaskState.revoked);

      await worker.stop();
      await app.close();
    });

    test('a live worker skips a task revoked while still queued', () async {
      final app = _app();
      var ran = false;
      app.task<void>('skip_me', (ctx) async => ran = true);
      final worker = app.worker()..run();

      final r = await app.send<void>('skip_me',
          eta: DateTime.now().add(const Duration(milliseconds: 300)));
      await r.revoke();

      await expectLater(
        r.get(timeout: const Duration(seconds: 2)),
        throwsA(isA<TaskFailureException>()),
      );
      expect(ran, isFalse);

      await worker.stop();
      await app.close();
    });

    test('get() rejects with TaskFailureException for a revoked task',
        () async {
      final app = _app();
      app.task<void>('irrelevant', (ctx) async => null);
      final r = await app.send<void>('irrelevant');
      await r.revoke();
      await expectLater(
        r.get(timeout: const Duration(seconds: 1)),
        throwsA(isA<TaskFailureException>()),
      );
      await app.close();
    });

    test('TaskContext.isRevoked lets a cooperative handler notice mid-run',
        () async {
      final app = _app();
      var observedRevoked = false;
      AsyncResult<void>? selfResult;
      app.task<void>('cooperative', (ctx) async {
        await Future<void>.delayed(const Duration(milliseconds: 100));
        observedRevoked = ctx.isRevoked;
        return null;
      });
      final worker = app.worker()..run();

      selfResult = await app.send<void>('cooperative');
      await Future<void>.delayed(const Duration(milliseconds: 20));
      await selfResult.revoke(); // published while the handler is mid-flight

      await Future<void>.delayed(const Duration(milliseconds: 300));
      expect(observedRevoked, isTrue);

      await worker.stop();
      await app.close();
    });
  });

  group('worker pause/resume', () {
    test('pausing a queue stops new tasks on it without affecting others',
        () async {
      final channel = InMemoryControlChannel();
      final app = _app(controlChannel: channel);
      var pausedRuns = 0;
      var activeRuns = 0;
      app.task<void>('on_paused', (ctx) async => pausedRuns++, queue: 'paused');
      app.task<void>('on_active', (ctx) async => activeRuns++, queue: 'active');
      final worker = app.worker(queues: ['paused', 'active'])..run();
      await channel.publish(const ControlMessage.pause('paused'));
      // The consumer loop's very first reserve() call started with the
      // pre-pause queue list and won't notice the pause until that slice
      // (bounded at 500ms) elapses and it loops again — wait it out so the
      // assertions below aren't racing that internal detail.
      await Future<void>.delayed(const Duration(milliseconds: 600));

      await app.send<void>('on_paused', queue: 'paused');
      final activeResult = await app.send<void>('on_active', queue: 'active');

      await activeResult.get(timeout: const Duration(seconds: 2));
      await Future<void>.delayed(const Duration(milliseconds: 200));
      expect(activeRuns, 1);
      expect(pausedRuns, 0, reason: 'paused queue must not be drained');

      await channel.publish(const ControlMessage.resume('paused'));
      await Future<void>.delayed(const Duration(milliseconds: 300));
      expect(pausedRuns, 1, reason: 'resumed queue should drain normally');

      await worker.stop();
      await app.close();
    });
  });
}
