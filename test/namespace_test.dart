import 'dart:async';

import 'package:gisila_queue/gisila_queue.dart';
import 'package:test/test.dart';

/// An in-memory broker that reports a namespace, standing in for a Redis
/// or RabbitMQ broker without a server.
class _NamespacedBroker extends InMemoryBroker implements Namespaced {
  _NamespacedBroker(this.prefix)
      : super(pollInterval: const Duration(milliseconds: 5));

  @override
  final String prefix;
}

/// Records messages the worker puts back with an ETA.
class _CapturingBroker extends InMemoryBroker {
  _CapturingBroker() : super(pollInterval: const Duration(milliseconds: 5));

  final requeued = <TaskMessage>[];
  final requeuedAt = <String, DateTime>{};

  @override
  Future<void> enqueue(TaskMessage message) async {
    if (message.eta != null) {
      requeued.add(message);
      requeuedAt[message.id] = DateTime.now();
    }
    return super.enqueue(message);
  }
}

class _RecordingLogger extends QueueLogger {
  final warnings = <String>[];

  @override
  void log(LogLevel level, String message, {Object? error, StackTrace? stack}) {
    if (level == LogLevel.warning) warnings.add(message);
  }
}

void main() {
  group('namespaces (Q-12)', () {
    test('GisilaQueue.redis puts every component in the namespace', () {
      final app = GisilaQueue.redis(namespace: 'billing', events: true);
      expect(app.namespace, 'billing');
      expect((app.broker as RedisBroker).prefix, 'billing');
      expect((app.backend as RedisResultBackend).prefix, 'billing');
      expect((app.controlChannel as RedisControlChannel).prefix, 'billing');
      expect((app.events as RedisEventSink).prefix, 'billing');

      final beat = Beat(app);
      expect((beat.lock as RedisBeatLock).prefix, 'billing');
      expect((beat.stateStore as RedisBeatStateStore).prefix, 'billing');
      expect(beat.namespace, 'billing');
    });

    test('GisilaQueue.redis rejects an empty namespace', () {
      expect(() => GisilaQueue.redis(namespace: ''), throwsArgumentError);
      expect(() => GisilaQueue.redis(namespace: 'my app'), throwsArgumentError);
    });

    test('components with different prefixes are refused', () {
      expect(
        () => GisilaQueue(
          broker: RedisBroker(prefix: 'billing'),
          backend: RedisResultBackend(), // forgot the prefix: 'gq'
        ),
        throwsA(isA<ArgumentError>().having((e) => e.message, 'message',
            allOf(contains('broker="billing"'), contains('backend="gq"')))),
      );
    });

    test("a beat lock outside the app's namespace is refused", () {
      final app = GisilaQueue(broker: RedisBroker(prefix: 'billing'));
      expect(() => Beat(app, lock: RedisBeatLock()), throwsArgumentError);
      expect(Beat(app, lock: RedisBeatLock(prefix: 'billing')).namespace,
          'billing');
    });

    test('in-memory apps have no namespace and log no warning', () async {
      final logger = _RecordingLogger();
      final app = GisilaQueue(broker: InMemoryBroker(), logger: logger);
      expect(app.namespace, isNull);
      final worker =
          app.worker(reserveTimeout: const Duration(milliseconds: 20));
      unawaited(worker.run());
      await worker.stop();
      expect(logger.warnings, isEmpty);
    });

    test('a worker on the default namespace warns at startup', () async {
      final logger = _RecordingLogger();
      final app = GisilaQueue(
          broker: _NamespacedBroker(defaultNamespace), logger: logger);
      final worker =
          app.worker(reserveTimeout: const Duration(milliseconds: 20));
      unawaited(worker.run());
      await worker.stop();
      expect(logger.warnings.single, contains('default namespace "gq"'));
    });

    test('a beat on the default namespace warns at startup', () async {
      final logger = _RecordingLogger();
      final app = GisilaQueue(
          broker: _NamespacedBroker(defaultNamespace), logger: logger);
      final beat = Beat(app, tickInterval: const Duration(milliseconds: 10));
      unawaited(beat.run());
      await Future<void>.delayed(const Duration(milliseconds: 30));
      await beat.stop();
      expect(logger.warnings, [contains('default namespace "gq"')]);
    });

    test('a worker on its own namespace does not warn', () async {
      final logger = _RecordingLogger();
      final app =
          GisilaQueue(broker: _NamespacedBroker('billing'), logger: logger);
      final worker =
          app.worker(reserveTimeout: const Duration(milliseconds: 20));
      unawaited(worker.run());
      await worker.stop();
      expect(logger.warnings, isEmpty);
    });

    test("a task another app's worker took is still run by its own app",
        () async {
      // Two apps misconfigured onto one namespace share the broker and the
      // result backend.
      final broker = _NamespacedBroker(defaultNamespace);
      final backend = InMemoryResultBackend();
      final billing = GisilaQueue(
          broker: broker, backend: backend, logger: SilentQueueLogger());
      final shop = GisilaQueue(
          broker: broker, backend: backend, logger: SilentQueueLogger());
      shop.task<String>('shop.ship', (ctx) async => 'shipped');

      // Only billing's worker is up when shop sends its task.
      final billingWorker = billing.worker(
          unknownTaskBackoff: const Duration(milliseconds: 20))
        ..run();
      final r = await shop.send<String>('shop.ship');
      await Future<void>.delayed(const Duration(milliseconds: 60));
      expect((await backend.fetch(r.id))?.state, isNot(TaskState.failure));

      final shopWorker = shop.worker()..run();
      expect(await r.get(timeout: const Duration(seconds: 3)), 'shipped');
      await billingWorker.stop();
      await shopWorker.stop();
      expect(
          await broker.reserve([Worker.defaultDeadLetterQueue],
              timeout: const Duration(milliseconds: 50)),
          isNull);
    });

    test('unknown-task delays double up to a minute', () async {
      final broker = _CapturingBroker();
      final app = GisilaQueue(broker: broker, logger: SilentQueueLogger());
      final worker = app.worker(
          unknownTaskBackoff: const Duration(seconds: 1), maxRequeues: 50);
      unawaited(worker.run());
      for (final requeues in [0, 3, 7]) {
        await broker.enqueue(TaskMessage(
            id: 'u$requeues',
            task: 'nobody',
            queue: app.defaultQueue,
            requeues: requeues));
      }
      await Future<void>.delayed(const Duration(milliseconds: 150));
      await worker.stop();
      final delays = {
        for (final m in broker.requeued)
          m.id: (m.eta!.difference(broker.requeuedAt[m.id]!).inMilliseconds /
                  1000)
              .round(),
      };
      // 1 s, 1 s << 3 = 8 s, 1 s << 7 = 128 s capped at 60 s.
      expect(delays, {'u0': 1, 'u3': 8, 'u7': 60});
      expect(broker.requeued.map((m) => m.requeues), [1, 4, 8]);
    });
  });
}
