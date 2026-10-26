import 'dart:async';

import 'package:gisila_queue/gisila_queue.dart';
import 'package:test/test.dart';

/// In-memory broker that pretends to have a visibility timeout, records
/// every [touch], and can be told to fail its next [ack].
class _LeasingBroker extends InMemoryBroker {
  _LeasingBroker() : super(pollInterval: const Duration(milliseconds: 5));

  final List<String> touched = [];
  int failNextAcks = 0;

  @override
  Duration? get visibilityTimeout => const Duration(milliseconds: 90);

  @override
  Future<void> touch(TaskMessage message) async => touched.add(message.id);

  @override
  Future<void> ack(TaskMessage message) async {
    if (failNextAcks > 0) {
      failNextAcks--;
      throw StateError('broker connection lost');
    }
  }
}

void main() {
  test('a long-running task keeps its reservation alive until it finishes',
      () async {
    final broker = _LeasingBroker();
    final app = GisilaQueue(
      broker: broker,
      backend: InMemoryResultBackend(),
      logger: SilentQueueLogger(),
    );
    app.task<void>('slow', (ctx) async {
      await Future<void>.delayed(const Duration(milliseconds: 400));
      return null;
    });
    final worker = app.worker()..run();

    final r = await app.send<void>('slow');
    await r.get(timeout: const Duration(seconds: 3));
    final touchesWhileRunning = broker.touched.length;
    // ~400ms at a 30ms interval: comfortably more than a couple.
    expect(touchesWhileRunning, greaterThanOrEqualTo(3));
    expect(broker.touched.toSet(), {r.id});

    // The lease stops once the task is done.
    await Future<void>.delayed(const Duration(milliseconds: 150));
    expect(broker.touched.length, touchesWhileRunning);

    await worker.stop();
    await app.close();
  });

  test('an error outside the handler does not kill the worker slot', () async {
    final broker = _LeasingBroker()..failNextAcks = 1;
    final app = GisilaQueue(
      broker: broker,
      backend: InMemoryResultBackend(),
      logger: SilentQueueLogger(),
    );
    final ran = <int>[];
    app.task<void>('note', (ctx) async {
      ran.add(ctx.args.first as int);
      return null;
    });
    // One slot: if the failed ack ended it, the second task would never run.
    final worker = app.worker(concurrency: 1)..run();

    await app.send<void>('note', args: [1]);
    final second = await app.send<void>('note', args: [2]);
    await second.get(timeout: const Duration(seconds: 5));
    expect(ran, [1, 2]);

    await worker.stop();
    await app.close();
  });

  test('deliveryTag is not serialized or copied', () {
    final m = TaskMessage(id: 'x', task: 't', queue: 'q').withDeliveryTag('d1');
    expect(m.deliveryTag, 'd1');
    expect(TaskMessage.decode(m.encode()).deliveryTag, isNull);
    expect(m.redelivered().deliveryTag, isNull);
  });
}
