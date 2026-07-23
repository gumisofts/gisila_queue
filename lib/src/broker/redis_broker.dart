import 'dart:async';

import 'package:redis/redis.dart';

import '../message.dart';
import 'broker.dart';

/// Distributed [Broker] backed by Redis, compatible with multiple worker
/// processes across machines.
///
/// Layout:
///
/// * Each queue is a Redis list at `<prefix>:queue:<name>`. Producers
///   `LPUSH` and workers `BRPOP`, giving FIFO order and blocking reservation
///   without polling.
/// * Delayed (ETA) messages live in a single sorted set
///   `<prefix>:scheduled`, scored by their due time in epoch millis. Before
///   each blocking pop a worker promotes any due messages into their list.
/// * Reserved-but-unacked messages are tracked in a sorted set
///   `<prefix>:unacked` (scored by reservation time) plus a hash
///   `<prefix>:unacked:payload` holding their encoded body, so a message
///   survives the crash of the worker that reserved it: [reserve] promotes
///   it into this ledger, [ack] removes it, and any entry left unacked past
///   [visibilityTimeout] is redelivered onto its original queue by the next
///   caller of [reserve]. This gives at-least-once delivery — the only gap
///   is the brief, unavoidable window between `BRPOP` removing a message
///   from its queue and the ledger write that follows it; a crash exactly
///   there (as opposed to anywhere during task execution, which is now
///   covered) can still lose a message, same trade-off Celery's own Redis
///   transport makes.
///
/// Two connections are used: one dedicated to the blocking `BRPOP`
/// (so it never stalls producers) and one for everything else. Both
/// reconnect transparently and retry once if a call fails (e.g. after a
/// Redis restart).
class RedisBroker extends Broker {
  RedisBroker({
    this.host = 'localhost',
    this.port = 6379,
    this.prefix = 'gq',
    this.visibilityTimeout = const Duration(minutes: 5),
  });

  final String host;
  final int port;

  /// Key namespace prefix shared by every queue/scheduled key.
  final String prefix;

  /// How long a reserved message may go unacked before it's assumed the
  /// worker that reserved it died, and is redelivered onto its queue.
  final Duration visibilityTimeout;

  RedisConnection? _producerConn;
  RedisConnection? _consumerConn;
  Command? _producer;
  Command? _consumer;
  Future<Command>? _producerConnecting;
  Future<Command>? _consumerConnecting;

  String _queueKey(String name) => '$prefix:queue:$name';
  String get _scheduledKey => '$prefix:scheduled';
  String get _unackedKey => '$prefix:unacked';
  String get _unackedPayloadKey => '$prefix:unacked:payload';

  Future<Command> _producerCmd() {
    final cached = _producer;
    if (cached != null) return Future.value(cached);
    return _producerConnecting ??= _doConnectProducer();
  }

  Future<Command> _doConnectProducer() async {
    try {
      final conn = RedisConnection();
      final cmd = await conn.connect(host, port);
      _producerConn = conn;
      _producer = cmd;
      return cmd;
    } finally {
      _producerConnecting = null;
    }
  }

  Future<Command> _consumerCmd() {
    final cached = _consumer;
    if (cached != null) return Future.value(cached);
    return _consumerConnecting ??= _doConnectConsumer();
  }

  Future<Command> _doConnectConsumer() async {
    try {
      final conn = RedisConnection();
      final cmd = await conn.connect(host, port);
      _consumerConn = conn;
      _consumer = cmd;
      return cmd;
    } finally {
      _consumerConnecting = null;
    }
  }

  void _resetProducer() {
    final conn = _producerConn;
    _producer = null;
    _producerConnecting = null;
    _producerConn = null;
    unawaited(conn?.close());
  }

  void _resetConsumer() {
    final conn = _consumerConn;
    _consumer = null;
    _consumerConnecting = null;
    _consumerConn = null;
    unawaited(conn?.close());
  }

  /// Runs [body] against the producer connection, reconnecting and retrying
  /// once if the cached connection has gone stale.
  Future<T> _withProducer<T>(Future<T> Function(Command) body) async {
    final cmd = await _producerCmd();
    try {
      return await body(cmd);
    } catch (_) {
      _resetProducer();
      final retryCmd = await _producerCmd();
      return await body(retryCmd);
    }
  }

  /// Runs [body] against the dedicated blocking connection, with the same
  /// reconnect-and-retry-once behaviour as [_withProducer].
  Future<T> _withConsumer<T>(Future<T> Function(Command) body) async {
    final cmd = await _consumerCmd();
    try {
      return await body(cmd);
    } catch (_) {
      _resetConsumer();
      final retryCmd = await _consumerCmd();
      return await body(retryCmd);
    }
  }

  @override
  Future<void> enqueue(TaskMessage message) => _withProducer((cmd) async {
        if (message.isDelayed(DateTime.now())) {
          final score = message.eta!.millisecondsSinceEpoch;
          await cmd.send_object(
              <Object>['ZADD', _scheduledKey, '$score', message.encode()]);
        } else {
          await cmd.send_object(
              <Object>['LPUSH', _queueKey(message.queue), message.encode()]);
        }
      });

  /// Moves every due message out of the scheduled set into its queue. Uses
  /// `ZREM`-before-`LPUSH` so that under concurrent workers only the worker
  /// that successfully removes a member is the one that enqueues it.
  Future<void> _promoteDue() => _withProducer((cmd) async {
        final now = DateTime.now().millisecondsSinceEpoch;
        final due = await cmd.send_object(
            <Object>['ZRANGEBYSCORE', _scheduledKey, '-inf', '$now']);
        if (due is! List) return;
        for (final raw in due) {
          final member = raw.toString();
          final removed =
              await cmd.send_object(<Object>['ZREM', _scheduledKey, member]);
          final count =
              removed is int ? removed : int.tryParse('$removed') ?? 0;
          if (count >= 1) {
            try {
              final msg = TaskMessage.decode(member);
              await cmd
                  .send_object(<Object>['LPUSH', _queueKey(msg.queue), member]);
            } catch (_) {
              // Malformed scheduled entry — drop it rather than block ETA
              // promotion for everything else.
            }
          }
        }
      });

  /// Redelivers any message that was reserved (see [reserve]) but never
  /// [ack]ed within [visibilityTimeout] — almost always because the worker
  /// holding it crashed or was killed mid-task.
  Future<void> _reapStale() => _withProducer((cmd) async {
        final deadline =
            DateTime.now().subtract(visibilityTimeout).millisecondsSinceEpoch;
        final stale = await cmd.send_object(
            <Object>['ZRANGEBYSCORE', _unackedKey, '-inf', '$deadline']);
        if (stale is! List || stale.isEmpty) return;
        for (final raw in stale) {
          final id = raw.toString();
          // ZREM first: only the caller that actually removes the member
          // gets to redeliver it, so concurrent reapers can't double-deliver.
          final removed =
              await cmd.send_object(<Object>['ZREM', _unackedKey, id]);
          final count =
              removed is int ? removed : int.tryParse('$removed') ?? 0;
          if (count < 1) continue;
          final payload =
              await cmd.send_object(<Object>['HGET', _unackedPayloadKey, id]);
          await cmd.send_object(<Object>['HDEL', _unackedPayloadKey, id]);
          if (payload == null) continue;
          try {
            final msg = TaskMessage.decode(payload.toString()).redelivered();
            await cmd.send_object(
                <Object>['LPUSH', _queueKey(msg.queue), msg.encode()]);
          } catch (_) {
            // Malformed ledger entry — nothing sane to redeliver.
          }
        }
      });

  @override
  Future<TaskMessage?> reserve(List<String> queues,
      {required Duration timeout}) async {
    await _promoteDue();
    await _reapStale();
    // BRPOP timeout is whole seconds; clamp to >= 1 so we still park.
    final seconds = timeout.inSeconds < 1 ? 1 : timeout.inSeconds;
    final result = await _withConsumer((cmd) => cmd.send_object(<Object>[
          'BRPOP',
          ...queues.map(_queueKey),
          '$seconds',
        ]));
    if (result is! List || result.length != 2) return null;
    final payload = result[1].toString();
    final message = TaskMessage.decode(payload);
    // Record the message as in-flight *before* returning it, so a crash
    // anywhere during task execution still leaves it recoverable via
    // _reapStale once visibilityTimeout elapses.
    await _withProducer((cmd) async {
      final now = DateTime.now().millisecondsSinceEpoch;
      await cmd.send_object(<Object>['ZADD', _unackedKey, '$now', message.id]);
      await cmd.send_object(
          <Object>['HSET', _unackedPayloadKey, message.id, payload]);
    });
    return message;
  }

  @override
  Future<void> ack(TaskMessage message) => _withProducer((cmd) async {
        await cmd.send_object(<Object>['ZREM', _unackedKey, message.id]);
        await cmd.send_object(<Object>['HDEL', _unackedPayloadKey, message.id]);
      });

  @override
  Future<int> length(String queue) => _withProducer((cmd) async {
        final len = await cmd.send_object(<Object>['LLEN', _queueKey(queue)]);
        return len is int ? len : int.tryParse('$len') ?? 0;
      });

  @override
  Future<int> purge(String queue) => _withProducer((cmd) async {
        final len = await cmd.send_object(<Object>['LLEN', _queueKey(queue)]);
        final n = len is int ? len : int.tryParse('$len') ?? 0;
        if (n > 0) await cmd.send_object(<Object>['DEL', _queueKey(queue)]);
        return n;
      });

  /// Number of messages currently reserved by a worker but not yet acked.
  Future<int> unackedCount() => _withProducer((cmd) async {
        final len = await cmd.send_object(<Object>['ZCARD', _unackedKey]);
        return len is int ? len : int.tryParse('$len') ?? 0;
      });

  @override
  Future<void> close() async {
    await _producerConn?.close();
    await _consumerConn?.close();
    _producer = _consumer = null;
    _producerConn = _consumerConn = null;
    _producerConnecting = _consumerConnecting = null;
  }
}
