import 'dart:async';

import 'package:redis/redis.dart';
import 'package:uuid/uuid.dart';

import '../message.dart';
import 'broker.dart';

/// Distributed [Broker] backed by Redis, compatible with multiple worker
/// processes across machines.
///
/// Layout:
///
/// * Each queue is a Redis list at `<prefix>:queue:<name>`. Producers
///   `LPUSH` and workers pop from the other end, giving FIFO order.
/// * Each queue also has a notify list `<prefix>:notify:<name>`; producers
///   push a token there, and idle workers `BLPOP` the notify lists so they
///   wake as soon as work arrives instead of polling.
/// * Delayed (ETA) messages live in a single sorted set
///   `<prefix>:scheduled`, scored by their due time in epoch millis. Before
///   each reserve a worker promotes any due messages into their list.
/// * Reserved-but-unacked messages are tracked in a sorted set
///   `<prefix>:unacked` plus a hash `<prefix>:unacked:payload` holding their
///   encoded body, both keyed by a fresh per-delivery token
///   ([TaskMessage.deliveryTag]) and scored by the last time the worker
///   holding it was heard from. [reserve] pops a message and writes the
///   ledger in one Lua script, [touch] refreshes the score while the task
///   runs, and [ack] removes exactly that delivery. Any entry not touched
///   within [visibilityTimeout] is redelivered onto its original queue by
///   the next caller of [reserve]. Every move between keys is a single
///   script, so a crash at any point leaves the message somewhere
///   recoverable: delivery is at-least-once.
/// * Messages that can't be decoded are pushed onto `<prefix>:dead`
///   (capped at [deadLetterLimit]) instead of being dropped.
///
/// The scripts touch keys derived from message contents, so this broker
/// targets a single Redis (or a primary/replica pair), not Redis Cluster.
///
/// Two connections are used: one dedicated to the blocking `BLPOP`
/// (so it never stalls producers) and one for everything else. Both
/// reconnect transparently and retry once if a call fails (e.g. after a
/// Redis restart).
class RedisBroker extends Broker {
  RedisBroker({
    this.host = 'localhost',
    this.port = 6379,
    this.prefix = 'gq',
    this.visibilityTimeout = const Duration(minutes: 5),
    this.deadLetterLimit = 10000,
  });

  final String host;
  final int port;

  /// Key namespace prefix shared by every queue/scheduled key.
  final String prefix;

  /// How long a reserved message may go without an [ack] or [touch] before
  /// it's assumed the worker that reserved it died, and is redelivered onto
  /// its queue. Workers touch running tasks well inside this window, so it
  /// bounds crash-recovery latency, not task runtime.
  @override
  final Duration visibilityTimeout;

  /// Most entries kept on the dead-letter list; older ones are trimmed.
  final int deadLetterLimit;

  /// Most entries promoted or reaped per [reserve] call.
  static const _batch = 100;

  /// Most wake-up tokens kept per notify list. Extra tokens only cause a
  /// spurious wake-up, so this just bounds the list.
  static const _notifyCap = 256;

  static const _uuid = Uuid();

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
  String _notifyKey(String name) => '$prefix:notify:$name';
  String get _deadKey => '$prefix:dead';

  // KEYS: list, notify. ARGV: payload, notify cap.
  static const _pushScript = """
redis.call('LPUSH', KEYS[1], ARGV[1])
redis.call('LPUSH', KEYS[2], '1')
redis.call('LTRIM', KEYS[2], 0, tonumber(ARGV[2]) - 1)
return 1
""";

  // Pushes ARGV[2] onto KEYS[2] if ARGV[1] was still in sorted set KEYS[1].
  // KEYS: zset, list, notify. ARGV: member, payload, notify cap (0 = no
  // token), list cap (0 = uncapped).
  static const _moveScript = """
if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then return 0 end
redis.call('LPUSH', KEYS[2], ARGV[2])
if tonumber(ARGV[4]) > 0 then
  redis.call('LTRIM', KEYS[2], 0, tonumber(ARGV[4]) - 1)
end
if tonumber(ARGV[3]) > 0 then
  redis.call('LPUSH', KEYS[3], '1')
  redis.call('LTRIM', KEYS[3], 0, tonumber(ARGV[3]) - 1)
end
return 1
""";

  // Redelivers a stale reservation, unless it was acked or touched since.
  // KEYS: unacked, payloads, list, notify. ARGV: tag, deadline, payload,
  // notify cap (0 = no token), list cap (0 = uncapped).
  static const _reapScript = """
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not score or tonumber(score) > tonumber(ARGV[2]) then return 0 end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('LPUSH', KEYS[3], ARGV[3])
if tonumber(ARGV[5]) > 0 then
  redis.call('LTRIM', KEYS[3], 0, tonumber(ARGV[5]) - 1)
end
if tonumber(ARGV[4]) > 0 then
  redis.call('LPUSH', KEYS[4], '1')
  redis.call('LTRIM', KEYS[4], 0, tonumber(ARGV[4]) - 1)
end
return 1
""";

  // Pops the first ready message and records it as reserved under ARGV[2].
  // KEYS: unacked, payloads, queue lists in priority order. ARGV: now, tag.
  static const _reserveScript = """
for i = 3, #KEYS do
  local payload = redis.call('RPOP', KEYS[i])
  if payload then
    redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
    redis.call('HSET', KEYS[2], ARGV[2], payload)
    return payload
  end
end
return false
""";

  // Drops reservation ARGV[1]; with ARGV[2], dead-letters that payload.
  // KEYS: unacked, payloads, dead. ARGV: tag, [payload, dead cap].
  static const _ackScript = """
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
if ARGV[2] then
  redis.call('LPUSH', KEYS[3], ARGV[2])
  redis.call('LTRIM', KEYS[3], 0, tonumber(ARGV[3]) - 1)
end
return 1
""";

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
          await cmd.send_object(<Object>[
            'EVAL',
            _pushScript,
            '2',
            _queueKey(message.queue),
            _notifyKey(message.queue),
            message.encode(),
            '$_notifyCap',
          ]);
        }
      });

  /// Moves every due message out of the scheduled set into its queue. Each
  /// move is one script ([_moveScript]), so a crash can't drop a message
  /// between the `ZREM` and the `LPUSH`, and under concurrent workers only
  /// the one whose `ZREM` succeeds enqueues it. Undecodable entries go to
  /// the dead-letter list instead of vanishing.
  Future<void> _promoteDue() => _withProducer((cmd) async {
        final now = DateTime.now().millisecondsSinceEpoch;
        final due = await cmd.send_object(<Object>[
          'ZRANGEBYSCORE', _scheduledKey, '-inf', '$now', //
          'LIMIT', '0', '$_batch',
        ]);
        if (due is! List) return;
        for (final raw in due) {
          final member = raw.toString();
          String? queue;
          try {
            queue = TaskMessage.decode(member).queue;
          } catch (_) {}
          await cmd.send_object(<Object>[
            'EVAL',
            _moveScript,
            '3',
            _scheduledKey,
            queue == null ? _deadKey : _queueKey(queue),
            queue == null ? _deadKey : _notifyKey(queue),
            member,
            queue == null ? '0' : '$_notifyCap',
            queue == null ? '$deadLetterLimit' : '0',
          ]);
        }
      });

  /// Redelivers any message that was reserved (see [reserve]) but neither
  /// [ack]ed nor [touch]ed within [visibilityTimeout] — almost always
  /// because the worker holding it crashed or was killed mid-task.
  Future<void> _reapStale() => _withProducer((cmd) async {
        final deadline =
            DateTime.now().subtract(visibilityTimeout).millisecondsSinceEpoch;
        final stale = await cmd.send_object(<Object>[
          'ZRANGEBYSCORE', _unackedKey, '-inf', '$deadline', //
          'LIMIT', '0', '$_batch',
        ]);
        if (stale is! List || stale.isEmpty) return;
        for (final raw in stale) {
          final tag = raw.toString();
          final payload =
              await cmd.send_object(<Object>['HGET', _unackedPayloadKey, tag]);
          if (payload == null) {
            // Ledger entry with no body (left by an older version that wrote
            // the two halves separately): nothing to redeliver.
            await cmd.send_object(<Object>['ZREM', _unackedKey, tag]);
            continue;
          }
          String? queue;
          var body = payload.toString();
          try {
            final msg = TaskMessage.decode(body).redelivered();
            queue = msg.queue;
            body = msg.encode();
          } catch (_) {}
          // The script re-checks the score, so a reservation touched since
          // the ZRANGEBYSCORE above is left alone, and only one concurrent
          // reaper gets to redeliver it.
          await cmd.send_object(<Object>[
            'EVAL',
            _reapScript,
            '4',
            _unackedKey,
            _unackedPayloadKey,
            queue == null ? _deadKey : _queueKey(queue),
            queue == null ? _deadKey : _notifyKey(queue),
            tag,
            '$deadline',
            body,
            queue == null ? '0' : '$_notifyCap',
            queue == null ? '$deadLetterLimit' : '0',
          ]);
        }
      });

  /// Pops the first ready message from [queues] (in priority order) and
  /// records it in the unacked ledger in the same script, so there is no
  /// window where a message is off its queue but not yet recoverable.
  /// Returns null when every queue is empty.
  Future<TaskMessage?> _take(List<String> queues) async {
    while (true) {
      final tag = _uuid.v4();
      final now = DateTime.now().millisecondsSinceEpoch;
      final payload = await _withProducer((cmd) => cmd.send_object(<Object>[
            'EVAL',
            _reserveScript,
            '${queues.length + 2}',
            _unackedKey,
            _unackedPayloadKey,
            ...queues.map(_queueKey),
            '$now',
            tag,
          ]));
      if (payload == null) return null;
      final body = payload.toString();
      try {
        return TaskMessage.decode(body).withDeliveryTag(tag);
      } catch (_) {
        // Undecodable: park it on the dead-letter list rather than drop it
        // or hand it to a worker, then try the next message.
        await _release(tag, deadLetter: body);
      }
    }
  }

  @override
  Future<TaskMessage?> reserve(List<String> queues,
      {required Duration timeout}) async {
    await _promoteDue();
    await _reapStale();
    final ready = await _take(queues);
    if (ready != null) return ready;
    // Nothing ready: park on the queues' notify lists until a producer (or
    // a promotion/redelivery) signals new work, then try once more. A
    // message pushed without a signal (e.g. by an older producer) is still
    // picked up, just after the timeout instead of immediately.
    // BLPOP timeout is whole seconds; clamp to >= 1 so we still park.
    final seconds = timeout.inSeconds < 1 ? 1 : timeout.inSeconds;
    await _withConsumer((cmd) => cmd.send_object(<Object>[
          'BLPOP',
          ...queues.map(_notifyKey),
          '$seconds',
        ]));
    return _take(queues);
  }

  /// Removes delivery [tag] from the unacked ledger, optionally pushing
  /// [deadLetter] onto the dead-letter list in the same script.
  Future<void> _release(String tag, {String? deadLetter}) =>
      _withProducer((cmd) => cmd.send_object(<Object>[
            'EVAL',
            _ackScript,
            '3',
            _unackedKey,
            _unackedPayloadKey,
            _deadKey,
            tag,
            if (deadLetter != null) ...[deadLetter, '$deadLetterLimit'],
          ]));

  /// Acks exactly the delivery [message] came from (its
  /// [TaskMessage.deliveryTag]), so acking one copy never clears the
  /// reservation of another copy with the same id.
  @override
  Future<void> ack(TaskMessage message) =>
      _release(message.deliveryTag ?? message.id);

  /// Pushes the reservation's deadline forward. Only updates an existing
  /// entry (`XX`), so a touch racing an ack can't resurrect it.
  @override
  Future<void> touch(TaskMessage message) => _withProducer((cmd) async {
        final now = DateTime.now().millisecondsSinceEpoch;
        await cmd.send_object(<Object>[
          'ZADD',
          _unackedKey,
          'XX',
          '$now',
          message.deliveryTag ?? message.id,
        ]);
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

  /// Number of undecodable messages parked on the dead-letter list.
  Future<int> deadLetterCount() => _withProducer((cmd) async {
        final len = await cmd.send_object(<Object>['LLEN', _deadKey]);
        return len is int ? len : int.tryParse('$len') ?? 0;
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
