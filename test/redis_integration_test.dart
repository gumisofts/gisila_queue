@Tags(['redis'])
library;

import 'dart:async';
import 'dart:io';

import 'package:gisila_queue/gisila_queue.dart';
import 'package:redis/redis.dart' as redis;
import 'package:test/test.dart';

/// These tests talk to a real Redis instance (default localhost:6379,
/// override with GISILA_TEST_REDIS_HOST/PORT). They're tagged `redis` so
/// they can be excluded (`dart test --exclude-tags redis`) wherever a
/// broker isn't available; if the initial connectivity check fails, each
/// test skips itself instead of failing the run.
String get _host =>
    Platform.environment['GISILA_TEST_REDIS_HOST'] ?? 'localhost';
int get _port =>
    int.tryParse(Platform.environment['GISILA_TEST_REDIS_PORT'] ?? '') ?? 6379;

Future<bool> _redisReachable() async {
  try {
    final conn = redis.RedisConnection();
    final cmd = await conn.connect(_host, _port).timeout(
          const Duration(seconds: 2),
        );
    await cmd.send_object(<Object>['PING']);
    await conn.close();
    return true;
  } catch (_) {
    return false;
  }
}

/// Deletes every key under `prefix:*` — scoped cleanup, never a blanket
/// FLUSHDB, since this may be a shared Redis instance.
Future<void> _cleanupPrefix(String prefix) async {
  final conn = redis.RedisConnection();
  try {
    final cmd = await conn.connect(_host, _port);
    var cursor = '0';
    final keys = <String>[];
    do {
      final res =
          await cmd.send_object(<Object>['SCAN', cursor, 'MATCH', '$prefix:*']);
      if (res is! List || res.length != 2) break;
      cursor = res[0].toString();
      final batch = res[1];
      if (batch is List) keys.addAll(batch.map((e) => e.toString()));
    } while (cursor != '0');
    for (final key in keys) {
      await cmd.send_object(<Object>['DEL', key]);
    }
  } catch (_) {
    // best effort
  } finally {
    await conn.close();
  }
}

/// Returns the ids of every client connected right now, so a test can find
/// "which new connection just appeared" without disturbing others.
Future<Set<String>> _clientIds(redis.Command cmd) async {
  final raw = await cmd.send_object(<Object>['CLIENT', 'LIST']);
  final text = raw.toString();
  final ids = <String>{};
  for (final line in text.split('\n')) {
    final match = RegExp(r'id=(\d+)').firstMatch(line);
    if (match != null) ids.add(match.group(1)!);
  }
  return ids;
}

void main() {
  var reachable = false;

  setUpAll(() async {
    reachable = await _redisReachable();
    if (!reachable) {
      // ignore: avoid_print
      print('!! Redis not reachable at $_host:$_port — redis_integration_test '
          'will skip every case.');
    }
  });

  group('RedisBroker reliability', () {
    late String prefix;
    late RedisBroker broker;

    setUp(() {
      prefix = 'gq_itest_${DateTime.now().microsecondsSinceEpoch}';
      broker = RedisBroker(
        host: _host,
        port: _port,
        prefix: prefix,
        visibilityTimeout: const Duration(milliseconds: 300),
      );
    });

    tearDown(() async {
      await broker.close();
      await _cleanupPrefix(prefix);
    });

    test('ack removes a reserved message from the unacked ledger', () async {
      if (!reachable) return;
      await broker.enqueue(TaskMessage(id: 'a', task: 't', queue: 'q'));
      final msg =
          await broker.reserve(['q'], timeout: const Duration(seconds: 2));
      expect(msg, isNotNull);
      expect(await broker.unackedCount(), 1);
      await broker.ack(msg!);
      expect(await broker.unackedCount(), 0);
    });

    test('an unacked message is redelivered after the visibility timeout',
        () async {
      if (!reachable) return;
      await broker.enqueue(TaskMessage(id: 'crash-me', task: 't', queue: 'q'));
      final first =
          await broker.reserve(['q'], timeout: const Duration(seconds: 2));
      expect(first, isNotNull);
      expect(first!.redeliveries, 0);
      // Simulate the worker holding `first` dying: never ack it. Wait past
      // the visibility timeout, then reserve again — the reaper should
      // have put it back on the queue.
      await Future<void>.delayed(const Duration(milliseconds: 500));
      final redelivered =
          await broker.reserve(['q'], timeout: const Duration(seconds: 2));
      expect(redelivered, isNotNull);
      expect(redelivered!.id, 'crash-me');
      expect(redelivered.redeliveries, 1);
    });

    test('an acked message is never redelivered', () async {
      if (!reachable) return;
      await broker.enqueue(TaskMessage(id: 'done', task: 't', queue: 'q'));
      final msg =
          await broker.reserve(['q'], timeout: const Duration(seconds: 2));
      await broker.ack(msg!);
      await Future<void>.delayed(const Duration(milliseconds: 500));
      final again =
          await broker.reserve(['q'], timeout: const Duration(seconds: 1));
      expect(again, isNull);
    });

    test('a touched reservation is not redelivered', () async {
      if (!reachable) return;
      await broker.enqueue(TaskMessage(id: 'slow', task: 't', queue: 'q'));
      final held =
          await broker.reserve(['q'], timeout: const Duration(seconds: 2));
      for (var i = 0; i < 6; i++) {
        await Future<void>.delayed(const Duration(milliseconds: 100));
        await broker.touch(held!);
      }
      // 600ms held against a 300ms visibility timeout, but touched.
      final again =
          await broker.reserve(['q'], timeout: const Duration(seconds: 1));
      expect(again, isNull);
      expect(await broker.unackedCount(), 1);
    });

    test('acking one copy leaves another copy with the same id reserved',
        () async {
      if (!reachable) return;
      final m = TaskMessage(id: 'dup', task: 't', queue: 'q');
      await broker.enqueue(m);
      await broker.enqueue(m);
      final first =
          await broker.reserve(['q'], timeout: const Duration(seconds: 2));
      final second =
          await broker.reserve(['q'], timeout: const Duration(seconds: 2));
      expect(first!.deliveryTag, isNot(second!.deliveryTag));
      expect(await broker.unackedCount(), 2);
      await broker.ack(first);
      expect(await broker.unackedCount(), 1);
      await broker.ack(second);
      expect(await broker.unackedCount(), 0);
    });

    test('undecodable messages go to the dead-letter list', () async {
      if (!reachable) return;
      final conn = redis.RedisConnection();
      final cmd = await conn.connect(_host, _port);
      await cmd.send_object(<Object>['LPUSH', '$prefix:queue:q', 'not json']);
      final due = DateTime.now().millisecondsSinceEpoch - 1000;
      await cmd.send_object(
          <Object>['ZADD', '$prefix:scheduled', '$due', '{broken']);
      await conn.close();
      await broker.enqueue(TaskMessage(id: 'good', task: 't', queue: 'q'));

      final msg =
          await broker.reserve(['q'], timeout: const Duration(seconds: 2));
      expect(msg!.id, 'good');
      expect(await broker.deadLetterCount(), 2);
      expect(await broker.unackedCount(), 1);
    });

    test('an idle reserve wakes as soon as a message is enqueued', () async {
      if (!reachable) return;
      final sw = Stopwatch()..start();
      final pending =
          broker.reserve(['q'], timeout: const Duration(seconds: 5));
      await Future<void>.delayed(const Duration(milliseconds: 200));
      await broker.enqueue(TaskMessage(id: 'wake', task: 't', queue: 'q'));
      final msg = await pending;
      expect(msg!.id, 'wake');
      expect(sw.elapsed, lessThan(const Duration(seconds: 2)));
    });

    test('a worker runs a task longer than the visibility timeout once',
        () async {
      if (!reachable) return;
      final app = GisilaQueue(
        broker: broker,
        backend: InMemoryResultBackend(),
        logger: SilentQueueLogger(),
      );
      var runs = 0;
      app.task<void>('long', (ctx) async {
        runs++;
        await Future<void>.delayed(const Duration(milliseconds: 1200));
        return null;
      });
      // Two slots, so a redelivered copy would be picked up while the first
      // is still running.
      final worker = app.worker(concurrency: 2)..run();
      final r = await app.send<void>('long');
      await r.get(timeout: const Duration(seconds: 5));
      await Future<void>.delayed(const Duration(milliseconds: 600));
      expect(runs, 1);
      expect(await broker.unackedCount(), 0);
      await worker.stop();
    });

    test('reconnects transparently after the cached connection is killed',
        () async {
      if (!reachable) return;
      final admin = redis.RedisConnection();
      final adminCmd = await admin.connect(_host, _port);
      final before = await _clientIds(adminCmd);

      await broker.enqueue(TaskMessage(id: 'one', task: 't', queue: 'q'));

      final after = await _clientIds(adminCmd);
      final newIds = after.difference(before);
      expect(newIds, isNotEmpty,
          reason: 'expected the broker to open a new producer connection');
      for (final id in newIds) {
        await adminCmd.send_object(<Object>['CLIENT', 'KILL', 'ID', id]);
      }

      // The next call should transparently reconnect and succeed rather
      // than throwing, despite the cached connection now being dead.
      await broker.enqueue(TaskMessage(id: 'two', task: 't', queue: 'q'));
      expect(await broker.length('q'), 2);

      await admin.close();
    });
  });

  group('RedisResultBackend reliability', () {
    late String prefix;
    late RedisResultBackend backend;

    setUp(() {
      prefix = 'gq_itest_${DateTime.now().microsecondsSinceEpoch}';
      backend = RedisResultBackend(host: _host, port: _port, prefix: prefix);
    });

    tearDown(() async {
      await backend.close();
      await _cleanupPrefix(prefix);
    });

    test('reconnects transparently after the cached connection is killed',
        () async {
      if (!reachable) return;
      final admin = redis.RedisConnection();
      final adminCmd = await admin.connect(_host, _port);
      final before = await _clientIds(adminCmd);

      await backend.store(
          'id1',
          StoredResult(
              state: TaskState.success, result: 1, updatedAt: DateTime.now()));

      final after = await _clientIds(adminCmd);
      for (final id in after.difference(before)) {
        await adminCmd.send_object(<Object>['CLIENT', 'KILL', 'ID', id]);
      }

      final fetched = await backend.fetch('id1');
      expect(fetched?.result, 1);

      await admin.close();
    });
  });

  group('RedisResultBackend chord barrier', () {
    late String prefix;
    late RedisResultBackend backend;

    setUp(() {
      prefix = 'gq_itest_${DateTime.now().microsecondsSinceEpoch}';
      backend = RedisResultBackend(host: _host, port: _port, prefix: prefix);
    });

    tearDown(() async {
      await backend.close();
      await _cleanupPrefix(prefix);
    });

    test('chordArrive returns true for exactly one of many concurrent callers',
        () async {
      if (!reachable) return;
      const total = 20;
      final winners = await Future.wait(
          [for (var i = 0; i < total; i++) backend.chordArrive('c1', total)]);
      expect(winners.where((w) => w).length, 1);
    });

    test('chordAbort returns true for exactly one of many concurrent callers',
        () async {
      if (!reachable) return;
      final winners = await Future.wait(
          [for (var i = 0; i < 10; i++) backend.chordAbort('c2')]);
      expect(winners.where((w) => w).length, 1);
    });

    test('an abort before the barrier completes prevents it from firing',
        () async {
      if (!reachable) return;
      expect(await backend.chordAbort('c3'), isTrue);
      expect(await backend.chordArrive('c3', 1), isFalse);
    });
  });

  group('RedisControlChannel', () {
    late String prefix;

    setUp(() {
      prefix = 'gq_itest_${DateTime.now().microsecondsSinceEpoch}';
    });

    test('delivers a published message to every live subscriber', () async {
      if (!reachable) return;
      final publisher =
          RedisControlChannel(host: _host, port: _port, prefix: prefix);
      final subA =
          RedisControlChannel(host: _host, port: _port, prefix: prefix);
      final subB =
          RedisControlChannel(host: _host, port: _port, prefix: prefix);

      final receivedA = <ControlMessage>[];
      final receivedB = <ControlMessage>[];
      subA.subscribe().listen(receivedA.add);
      subB.subscribe().listen(receivedB.add);
      // Give the SUBSCRIBE calls a moment to register with Redis.
      await Future<void>.delayed(const Duration(milliseconds: 200));

      await publisher.publish(const ControlMessage.revoke('task-123'));
      await Future<void>.delayed(const Duration(milliseconds: 200));

      expect(receivedA, hasLength(1));
      expect(receivedA.single.type, ControlMessageType.revoke);
      expect(receivedA.single.taskId, 'task-123');
      expect(receivedB, hasLength(1));

      await publisher.close();
      await subA.close();
      await subB.close();
    });
  });

  group('RedisBeatLock', () {
    late String prefix;

    setUp(() {
      prefix = 'gq_itest_${DateTime.now().microsecondsSinceEpoch}';
    });

    tearDown(() async {
      await _cleanupPrefix(prefix);
    });

    test('only one owner holds the lock at a time', () async {
      if (!reachable) return;
      final lockA = RedisBeatLock(host: _host, port: _port, prefix: prefix);
      final lockB = RedisBeatLock(host: _host, port: _port, prefix: prefix);

      expect(await lockA.acquire('A', const Duration(seconds: 5)), isTrue);
      expect(await lockB.acquire('B', const Duration(seconds: 5)), isFalse);
      // A renewing (re-acquiring) its own lock still succeeds.
      expect(await lockA.acquire('A', const Duration(seconds: 5)), isTrue);

      await lockA.release('A');
      expect(await lockB.acquire('B', const Duration(seconds: 5)), isTrue);

      await lockA.close();
      await lockB.close();
    });
  });

  group('Beat with RedisBeatLock + RedisBeatStateStore', () {
    late String prefix;

    setUp(() {
      prefix = 'gq_itest_${DateTime.now().microsecondsSinceEpoch}';
    });

    tearDown(() async {
      await _cleanupPrefix(prefix);
    });

    test(
        'only the lock holder fires entries, and a new leader inherits '
        'the persisted last-run time instead of refiring immediately',
        () async {
      if (!reachable) return;
      final app = GisilaQueue(
        broker: InMemoryBroker(pollInterval: const Duration(milliseconds: 5)),
        logger: SilentQueueLogger(),
      );
      var runs = 0;
      app.task<void>('job', (ctx) async => runs++);
      final worker = app.worker()..run();
      Future<void> waitForRuns(int expected) async {
        final deadline = DateTime.now().add(const Duration(seconds: 2));
        while (runs < expected && DateTime.now().isBefore(deadline)) {
          await Future<void>.delayed(const Duration(milliseconds: 10));
        }
      }

      final beat1 = Beat(
        app,
        lock: RedisBeatLock(host: _host, port: _port, prefix: prefix),
        stateStore:
            RedisBeatStateStore(host: _host, port: _port, prefix: prefix),
        id: 'beat1',
      )..schedule(
          'every', 'job', Schedule.every(const Duration(milliseconds: 50)));
      final beat2 = Beat(
        app,
        lock: RedisBeatLock(host: _host, port: _port, prefix: prefix),
        stateStore:
            RedisBeatStateStore(host: _host, port: _port, prefix: prefix),
        id: 'beat2',
      )..schedule(
          'every', 'job', Schedule.every(const Duration(milliseconds: 50)));

      final t0 = DateTime.now();
      await beat1.tick(t0); // beat1 becomes leader and fires.
      await waitForRuns(1);
      expect(runs, 1);

      await beat2.tick(t0.add(const Duration(milliseconds: 10)));
      await waitForRuns(2); // give it a moment; expected to time out at 1.
      expect(runs, 1, reason: 'beat2 must not fire while beat1 holds the lock');

      await beat1.tick(t0.add(const Duration(milliseconds: 20)));
      expect(runs, 1, reason: 'not due yet within the 50ms interval');

      await beat1.stop(); // releases the lock.
      await beat2.tick(t0.add(const Duration(milliseconds: 60)));
      // beat2 already absorbed the persisted lastRun (t0) on its earlier
      // tick, so at t0+60ms (>= 50ms interval since t0) it fires exactly
      // once rather than treating itself as never-having-run.
      await waitForRuns(2);
      expect(runs, 2);

      await beat2.stop();
      await worker.stop();
      await app.close();
    });
  });
}
