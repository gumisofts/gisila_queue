import 'dart:async';

import 'package:gisila_queue/gisila_queue.dart';
import 'package:test/test.dart';

GisilaQueue _app() => GisilaQueue(
      broker: InMemoryBroker(pollInterval: const Duration(milliseconds: 5)),
      backend: InMemoryResultBackend(),
      logger: SilentQueueLogger(),
    );

Future<TaskMessage?> _dead(GisilaQueue app) =>
    app.broker.reserve([Worker.defaultDeadLetterQueue],
        timeout: const Duration(milliseconds: 300));

void main() {
  group('dead-letter queue (Q-09)', () {
    test('a task that fails for good is dead-lettered, and can be replayed',
        () async {
      final app = _app();
      var fixed = false;
      app.task<String>('flaky', (ctx) async {
        if (!fixed) throw StateError('db down');
        return 'ok';
      });
      final worker = app.worker(queues: ['reports'])..run();

      final r = await app.send<String>('flaky', queue: 'reports');
      await expectLater(r.get(timeout: const Duration(seconds: 2)),
          throwsA(isA<TaskFailureException>()));
      await worker.stop();

      final dead = await _dead(app);
      expect(dead, isNotNull);
      expect(dead!.id, r.id);
      expect(dead.deadLetter!['queue'], 'reports');
      expect(dead.deadLetter!['reason'], contains('db down'));
      await app.broker.enqueue(dead); // put it back for the replay

      fixed = true;
      expect(await app.replayDeadLetters(), 1);
      final w2 = app.worker(queues: ['reports'])..run();
      expect(await r.get(timeout: const Duration(seconds: 2)), 'ok');
      await w2.stop();
      expect(await _dead(app), isNull);
      await app.close();
    });

    test('an unknown task name is dead-lettered', () async {
      final app = _app();
      final worker = app.worker()..run();
      await app.broker.enqueue(TaskMessage(
          id: 'x1', task: 'nobody.handles.this', queue: app.defaultQueue));
      final dead = await app.broker.reserve([Worker.defaultDeadLetterQueue],
          timeout: const Duration(seconds: 2));
      expect(dead?.id, 'x1');
      expect(dead!.deadLetter!['reason'], contains('unknown task'));
      await worker.stop();
      await app.close();
    });

    test('RejectTask(requeue) stops looping after maxRequeues', () async {
      final app = _app();
      var runs = 0;
      app.task<void>('always-rejects', (ctx) async {
        runs++;
        throw const RejectTask(requeue: true, reason: 'not yet');
      });
      final worker = app.worker(maxRequeues: 3)..run();

      final r = await app.send<void>('always-rejects');
      final dead = await app.broker.reserve([Worker.defaultDeadLetterQueue],
          timeout: const Duration(seconds: 2));
      expect(dead?.id, r.id);
      expect(dead!.deadLetter!['reason'], contains('requeued 3 times'));
      expect(runs, 4);
      expect(await r.state, TaskState.failure);
      await worker.stop();
      await app.close();
    });

    test('a message redelivered too often is dead-lettered without running',
        () async {
      final app = _app();
      var ran = false;
      app.task<void>('crashes-workers', (ctx) async => ran = true);
      final worker = app.worker(maxRedeliveries: 2)..run();
      await app.broker.enqueue(TaskMessage(
          id: 'x2',
          task: 'crashes-workers',
          queue: app.defaultQueue,
          redeliveries: 3));
      final dead = await app.broker.reserve([Worker.defaultDeadLetterQueue],
          timeout: const Duration(seconds: 2));
      expect(dead?.id, 'x2');
      expect(ran, isFalse);
      await worker.stop();
      await app.close();
    });

    test('deadLetterQueue: null turns it off', () async {
      final app = _app();
      app.task<void>('fails', (ctx) async => throw StateError('no'));
      final worker = app.worker(deadLetterQueue: null)..run();
      final r = await app.send<void>('fails');
      await expectLater(r.get(timeout: const Duration(seconds: 2)),
          throwsA(isA<TaskFailureException>()));
      await worker.stop();
      expect(await _dead(app), isNull);
      await app.close();
    });

    test('dead-letter metadata survives the wire format', () {
      final m = TaskMessage(
        id: 'a',
        task: 't',
        queue: 'dead_letter',
        requeues: 2,
        deadLetter: {'reason': 'r', 'queue': 'q', 'at': 'now'},
      );
      final back = TaskMessage.decode(m.encode());
      expect(back.requeues, 2);
      expect(back.deadLetter, {'reason': 'r', 'queue': 'q', 'at': 'now'});
      expect(back.copyWith(clearDeadLetter: true).deadLetter, isNull);
    });
  });

  group('hard time limit (Q-10)', () {
    test('a timed-out handler keeps its slot until it really ends', () async {
      final app = _app();
      final events = <String>[];
      var sawAbandoned = false;
      app.task<void>('slow', (ctx) async {
        await Future<void>.delayed(const Duration(milliseconds: 300));
        sawAbandoned = ctx.isAbandoned;
        events.add('slow ended');
        return null;
      }, timeLimit: const Duration(milliseconds: 50));
      app.task<void>('next', (ctx) async => events.add('next started'));
      final worker = app.worker(concurrency: 1)..run();

      final slow = await app.send<void>('slow');
      await expectLater(slow.get(timeout: const Duration(seconds: 2)),
          throwsA(isA<TaskFailureException>()));
      final next = await app.send<void>('next');
      await next.get(timeout: const Duration(seconds: 2));

      // Before: the slot was freed at 50ms, so `next` ran while `slow` was
      // still going.
      expect(events, ['slow ended', 'next started']);
      expect(sawAbandoned, isTrue);
      await worker.stop();
      await app.close();
    });

    test('stop() does not wait forever on a stuck handler', () async {
      final app = _app();
      final stuck = Completer<void>();
      app.task<void>('stuck', (ctx) => stuck.future,
          timeLimit: const Duration(milliseconds: 50));
      final worker = app.worker()..run();
      final r = await app.send<void>('stuck');
      await expectLater(r.get(timeout: const Duration(seconds: 2)),
          throwsA(isA<TaskFailureException>()));
      await worker.stop().timeout(const Duration(seconds: 2));
      stuck.complete();
      await app.close();
    });
  });
}
