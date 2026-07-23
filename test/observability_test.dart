import 'dart:async';

import 'package:gisila_queue/gisila_queue.dart';
import 'package:gisila_queue/gisila_rose.dart';
import 'package:test/test.dart';

void main() {
  group('QueueEvent', () {
    test('round-trips through encode/decode', () {
      final e = QueueEvent(
        type: QueueEventType.taskSucceeded,
        timestamp: DateTime.parse('2026-01-01T00:00:00Z'),
        taskId: 'id1',
        taskName: 'math.add',
        queue: 'default',
        workerId: 'host:1',
        hostname: 'host:1',
        fields: {'runtime_ms': 12, 'result': 5},
      );
      final back = QueueEvent.decode(e.encode());
      expect(back.type, QueueEventType.taskSucceeded);
      expect(back.taskId, 'id1');
      expect(back.taskName, 'math.add');
      expect(back.workerId, 'host:1');
      expect(back.numField('runtime_ms'), 12);
      expect(back.fields['result'], 5);
      expect(back.timestamp.toUtc(), DateTime.parse('2026-01-01T00:00:00Z'));
    });
  });

  group('MonitorState.apply', () {
    test('folds a task lifecycle into a terminal success', () {
      final s = MonitorState();
      final ts = DateTime.parse('2026-01-01T00:00:00Z');
      s.apply(QueueEvent(
        type: QueueEventType.taskSent,
        timestamp: ts,
        taskId: 'a',
        taskName: 'job',
        queue: 'default',
        fields: {
          'args': [1],
          'max_retries': 2
        },
      ));
      s.apply(QueueEvent(
        type: QueueEventType.taskStarted,
        timestamp: ts.add(const Duration(milliseconds: 1)),
        taskId: 'a',
        taskName: 'job',
        workerId: 'w1',
      ));
      s.apply(QueueEvent(
        type: QueueEventType.taskSucceeded,
        timestamp: ts.add(const Duration(milliseconds: 5)),
        taskId: 'a',
        taskName: 'job',
        workerId: 'w1',
        fields: {'runtime_ms': 4, 'result': 'done'},
      ));

      final t = s.taskById('a')!;
      expect(t.state, TaskState.success);
      expect(t.queue, 'default');
      expect(t.workerId, 'w1');
      expect(t.runtimeMs, 4);
      expect(t.result, 'done');
      expect(t.args, [1]);
      expect(s.succeeded, 1);
      expect(s.sent, 1);
    });

    test('tracks worker liveness from heartbeats', () {
      final s = MonitorState(workerTimeout: const Duration(seconds: 30));
      final now = DateTime.parse('2026-01-01T00:00:00Z');
      s.apply(QueueEvent(
        type: QueueEventType.workerOnline,
        timestamp: now,
        workerId: 'w1',
        hostname: 'box:7',
        fields: {
          'queues': ['default'],
          'concurrency': 4
        },
      ));
      final w = s.workers.single;
      expect(w.id, 'w1');
      expect(w.queues, ['default']);
      expect(w.concurrency, 4);
      expect(w.isAlive(now, const Duration(seconds: 30)), isTrue);
      // Heartbeat lapses -> offline.
      expect(
        w.isAlive(
            now.add(const Duration(minutes: 1)), const Duration(seconds: 30)),
        isFalse,
      );

      s.apply(QueueEvent(
        type: QueueEventType.workerOffline,
        timestamp: now.add(const Duration(seconds: 1)),
        workerId: 'w1',
      ));
      expect(s.workers.single.online, isFalse);
    });

    test('evicts oldest tasks past maxTasks', () {
      final s = MonitorState(maxTasks: 2);
      for (var i = 0; i < 4; i++) {
        s.apply(QueueEvent(
          type: QueueEventType.taskSent,
          timestamp: DateTime.fromMillisecondsSinceEpoch(i),
          taskId: 't$i',
          taskName: 'job',
        ));
      }
      expect(s.taskCount, 2);
      expect(s.taskById('t0'), isNull);
      expect(s.taskById('t3'), isNotNull);
    });
  });

  group('end to end via InMemoryEventBus', () {
    test('monitor observes a real task run', () async {
      final bus = InMemoryEventBus();
      final monitor = MonitorState();
      await monitor.bind(bus);

      final app = GisilaQueue(
        broker: InMemoryBroker(pollInterval: const Duration(milliseconds: 5)),
        backend: InMemoryResultBackend(),
        events: bus,
        logger: SilentQueueLogger(),
      );
      final add = app.task<int>(
        'math.add',
        (ctx) async => (ctx.arg(0) as int) + (ctx.arg(1) as int),
      );
      final worker = app.worker()..run();

      final r = await add.delay([2, 3]);
      expect(await r.get(timeout: const Duration(seconds: 2)), 5);

      // Let the monitor's stream listeners drain.
      await Future<void>.delayed(const Duration(milliseconds: 50));

      final t = monitor.taskById(r.id);
      expect(t, isNotNull);
      expect(t!.state, TaskState.success);
      expect(t.result, 5);
      expect(monitor.succeeded, greaterThanOrEqualTo(1));
      expect(monitor.workers, isNotEmpty);

      await worker.stop();
      await app.close();
    });
  });
}
