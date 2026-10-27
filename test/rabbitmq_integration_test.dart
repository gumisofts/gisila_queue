@Tags(['rabbitmq'])
library;

import 'dart:async';
import 'dart:io';

import 'package:dart_amqp/dart_amqp.dart';
import 'package:gisila_queue/gisila_queue.dart';
import 'package:test/test.dart';

/// These tests talk to a real RabbitMQ instance (default localhost:5672,
/// override with GISILA_TEST_RABBITMQ_HOST/PORT). Tagged `rabbitmq` so they
/// can be excluded (`dart test --exclude-tags rabbitmq`) wherever a broker
/// isn't available; if the initial connectivity check fails, each test skips
/// itself instead of failing the run.
String get _host =>
    Platform.environment['GISILA_TEST_RABBITMQ_HOST'] ?? 'localhost';
int get _port =>
    int.tryParse(Platform.environment['GISILA_TEST_RABBITMQ_PORT'] ?? '') ??
    5672;

Future<bool> _rabbitReachable() async {
  try {
    final client = Client(
      settings: ConnectionSettings(
        host: _host,
        port: _port,
        maxConnectionAttempts: 1,
      ),
    );
    final channel = await client.channel().timeout(const Duration(seconds: 2));
    await channel.close();
    await client.close();
    return true;
  } catch (_) {
    return false;
  }
}

/// Deletes exchanges/queues under [prefix] — scoped cleanup, never a vhost
/// wipe, since this may be a shared RabbitMQ instance.
Future<void> _cleanupPrefix(String prefix) async {
  final client = Client(
    settings: ConnectionSettings(
      host: _host,
      port: _port,
      maxConnectionAttempts: 1,
    ),
  );
  try {
    final channel = await client.channel();
    for (final suffix in ['default', 'q', 'high', 'low', 'eta']) {
      try {
        final q = await channel.queue('$prefix.queue.$suffix', durable: true);
        await q.delete();
      } catch (_) {}
    }
    try {
      final scheduled = await channel.queue(
        '$prefix.scheduled',
        durable: true,
        arguments: <String, Object>{
          'x-dead-letter-exchange': '$prefix.tasks',
        },
      );
      await scheduled.delete();
    } catch (_) {}
    try {
      await (await channel.exchange(
        '$prefix.delay',
        ExchangeType.FANOUT,
        durable: true,
      ))
          .delete();
    } catch (_) {}
    try {
      await (await channel.exchange(
        '$prefix.tasks',
        ExchangeType.DIRECT,
        durable: true,
      ))
          .delete();
    } catch (_) {}
    await channel.close();
  } catch (_) {
    // best effort
  } finally {
    try {
      await client.close();
    } catch (_) {}
  }
}

void main() {
  var reachable = false;

  setUpAll(() async {
    reachable = await _rabbitReachable();
    if (!reachable) {
      // ignore: avoid_print
      print(
        '!! RabbitMQ not reachable at $_host:$_port — '
        'rabbitmq_integration_test will skip every case.',
      );
    }
  });

  group('RabbitBroker', () {
    late String prefix;
    late RabbitBroker broker;

    setUp(() {
      prefix = 'gq_rmq_${DateTime.now().microsecondsSinceEpoch}';
      broker = RabbitBroker(
        host: _host,
        port: _port,
        prefix: prefix,
        prefetchCount: 8,
      );
    });

    tearDown(() async {
      await broker.close();
      await _cleanupPrefix(prefix);
    });

    test('enqueue → reserve → ack round-trip', () async {
      if (!reachable) return;
      await broker.enqueue(
        TaskMessage(id: 'a1', task: 't', queue: 'q', args: [1]),
      );
      final msg =
          await broker.reserve(['q'], timeout: const Duration(seconds: 3));
      expect(msg, isNotNull);
      expect(msg!.id, 'a1');
      expect(msg.args, [1]);
      await broker.ack(msg);
      expect(
        await broker.reserve(['q'], timeout: const Duration(milliseconds: 300)),
        isNull,
      );
    });

    test('two deliveries with the same id are acked separately', () async {
      if (!reachable) return;
      final m = TaskMessage(id: 'dup', task: 't', queue: 'q');
      await broker.enqueue(m);
      await broker.enqueue(m);
      final first =
          await broker.reserve(['q'], timeout: const Duration(seconds: 3));
      final second =
          await broker.reserve(['q'], timeout: const Duration(seconds: 3));
      expect(first!.deliveryTag, isNot(second!.deliveryTag));
      await broker.ack(first);
      await broker.ack(second);
      // If either ack had hit the wrong delivery, one would still be
      // unacked and come back once this connection closes.
      await broker.close();
      broker = RabbitBroker(
        host: _host,
        port: _port,
        prefix: prefix,
        prefetchCount: 8,
      );
      expect(
        await broker.reserve(['q'], timeout: const Duration(seconds: 1)),
        isNull,
      );
    });

    test('multi-queue priority: higher queues drain first', () async {
      if (!reachable) return;
      // Start consumers on both queues while empty so later publishes land
      // in the inbox together (needed for priority ordering).
      expect(
        await broker.reserve(
          ['high', 'low'],
          timeout: const Duration(milliseconds: 200),
        ),
        isNull,
      );

      await broker.enqueue(TaskMessage(id: 'low1', task: 't', queue: 'low'));
      await broker.enqueue(TaskMessage(id: 'high1', task: 't', queue: 'high'));
      await Future<void>.delayed(const Duration(milliseconds: 150));

      final first = await broker.reserve(
        ['high', 'low'],
        timeout: const Duration(seconds: 3),
      );
      expect(first?.id, 'high1');
      await broker.ack(first!);

      final second = await broker.reserve(
        ['high', 'low'],
        timeout: const Duration(seconds: 3),
      );
      expect(second?.id, 'low1');
      await broker.ack(second!);
    });

    test('ETA message becomes available after delay via TTL+DLX', () async {
      if (!reachable) return;
      final eta = DateTime.now().add(const Duration(milliseconds: 800));
      await broker.enqueue(
        TaskMessage(id: 'later', task: 't', queue: 'eta', eta: eta),
      );

      expect(
        await broker.reserve(
          ['eta'],
          timeout: const Duration(milliseconds: 200),
        ),
        isNull,
      );

      final msg = await broker.reserve(
        ['eta'],
        timeout: const Duration(seconds: 5),
      );
      expect(msg, isNotNull);
      expect(msg!.id, 'later');
      await broker.ack(msg);
    });

    test('a long delay does not hold up a short one (Q-05)', () async {
      if (!reachable) return;
      final now = DateTime.now();
      await broker.enqueue(TaskMessage(
          id: 'hour',
          task: 't',
          queue: 'eta2',
          eta: now.add(const Duration(hours: 1))));
      await broker.enqueue(TaskMessage(
          id: 'soon',
          task: 't',
          queue: 'eta2',
          eta: now.add(const Duration(milliseconds: 700))));

      final msg =
          await broker.reserve(['eta2'], timeout: const Duration(seconds: 5));
      expect(msg?.id, 'soon');
      await broker.ack(msg!);
    });

    test('a delay spanning several buckets arrives on time, not early',
        () async {
      if (!reachable) return;
      final eta = DateTime.now().add(const Duration(milliseconds: 1700));
      await broker.enqueue(
          TaskMessage(id: 'multi', task: 't', queue: 'eta3', eta: eta));
      final msg =
          await broker.reserve(['eta3'], timeout: const Duration(seconds: 6));
      expect(msg?.id, 'multi');
      final early = eta.difference(DateTime.now()).inMilliseconds;
      expect(early, lessThan(RabbitBroker.minDelayBucketMs));
      await broker.ack(msg!);
    });

    test('unacked message is redelivered after broker close', () async {
      if (!reachable) return;
      await broker.enqueue(
        TaskMessage(id: 'crash-me', task: 't', queue: 'q'),
      );
      final first =
          await broker.reserve(['q'], timeout: const Duration(seconds: 3));
      expect(first?.id, 'crash-me');
      // Simulate worker crash: close without ack (rejects with requeue).
      await broker.close();

      final broker2 = RabbitBroker(
        host: _host,
        port: _port,
        prefix: prefix,
        prefetchCount: 8,
      );
      addTearDown(() async {
        await broker2.close();
      });

      final again = await broker2.reserve(
        ['q'],
        timeout: const Duration(seconds: 5),
      );
      expect(again, isNotNull);
      expect(again!.id, 'crash-me');
      await broker2.ack(again);
    });

    test('length and purge', () async {
      if (!reachable) return;
      await broker.enqueue(TaskMessage(id: 'p1', task: 't', queue: 'q'));
      await broker.enqueue(TaskMessage(id: 'p2', task: 't', queue: 'q'));
      // Publish is async on the wire; wait until both are visible.
      var depth = 0;
      for (var i = 0; i < 20; i++) {
        depth = await broker.length('q');
        if (depth >= 2) break;
        await Future<void>.delayed(const Duration(milliseconds: 50));
      }
      expect(depth, greaterThanOrEqualTo(2));

      final purged = await broker.purge('q');
      expect(purged, greaterThanOrEqualTo(2));
      expect(await broker.length('q'), 0);
    });

    test('end-to-end worker processes RabbitMQ tasks', () async {
      if (!reachable) return;
      final app = GisilaQueue(
        broker: broker,
        backend: InMemoryResultBackend(),
        logger: SilentQueueLogger(),
      );
      final add = app.task<int>(
        'math.add',
        (ctx) async => (ctx.arg(0) as int) + (ctx.arg(1) as int),
      );
      final worker = app.worker(concurrency: 2)..run();
      addTearDown(() async {
        await worker.stop();
      });

      final result = await add.delay([4, 5]);
      expect(
        await result.get(timeout: const Duration(seconds: 5)),
        9,
      );
    });
  });
}
