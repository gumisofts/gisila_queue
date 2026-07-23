import 'dart:async';

import 'package:redis/redis.dart';

import 'event.dart';
import 'event_sink.dart';

/// Shared key/channel naming for the Redis event transport.
mixin _RedisEventKeys {
  String get prefix;

  /// Pub/sub channel carrying the live event firehose.
  String get channel => '$prefix:events';

  /// Capped list holding the most recent events for dashboard backfill.
  String get historyKey => '$prefix:events:history';
}

/// Distributed [EventSink] that publishes events to a Redis pub/sub channel
/// (for live dashboards) and trims a capped history list (so a dashboard that
/// starts later still backfills recent activity).
///
/// Attach it to a [GisilaQueue] used by producers and workers:
///
/// ```dart
/// final events = RedisEventSink(host: 'localhost');
/// final app = GisilaQueue(broker: ..., events: events);
/// ```
///
/// A separate [RedisEventSource] (used by [GisilaRose]) reads the stream.
class RedisEventSink with _RedisEventKeys implements QueueEventSink {
  RedisEventSink({
    this.host = 'localhost',
    this.port = 6379,
    this.prefix = 'gq',
    this.historySize = 5000,
  });

  final String host;
  final int port;

  @override
  final String prefix;

  /// How many recent events Redis retains for backfill.
  final int historySize;

  RedisConnection? _conn;
  Command? _cmd;
  Future<Command>? _connecting;

  Future<Command> _command() {
    if (_cmd != null) return Future.value(_cmd!);
    return _connecting ??= () async {
      final conn = RedisConnection();
      final cmd = await conn.connect(host, port);
      _conn = conn;
      _cmd = cmd;
      return cmd;
    }();
  }

  @override
  Future<void> emit(QueueEvent event) async {
    try {
      final cmd = await _command();
      final payload = event.encode();
      await cmd.send_object(<Object>['PUBLISH', channel, payload]);
      await cmd.send_object(<Object>['LPUSH', historyKey, payload]);
      await cmd.send_object(
          <Object>['LTRIM', historyKey, '0', '${historySize - 1}']);
    } catch (_) {
      // Observability must never break task processing — drop on transport
      // failure and let the next event reconnect.
      _cmd = null;
      _connecting = null;
    }
  }

  @override
  Future<void> close() async {
    await _conn?.close();
    _conn = null;
    _cmd = null;
    _connecting = null;
  }
}

/// Read side of the Redis event transport, consumed by [GisilaRose].
///
/// Uses one connection for the blocking pub/sub [subscribe] and a second for
/// the [history] backfill (`LRANGE`), since a connection in pub/sub mode can
/// no longer issue ordinary commands.
class RedisEventSource with _RedisEventKeys implements QueueEventSource {
  RedisEventSource({
    this.host = 'localhost',
    this.port = 6379,
    this.prefix = 'gq',
  });

  final String host;
  final int port;

  @override
  final String prefix;

  RedisConnection? _subConn;
  RedisConnection? _histConn;
  Command? _histCmd;
  StreamController<QueueEvent>? _controller;

  @override
  Stream<QueueEvent> events() {
    final existing = _controller;
    if (existing != null) return existing.stream;
    final controller = StreamController<QueueEvent>.broadcast(
      onCancel: () {},
    );
    _controller = controller;
    _startSubscription(controller);
    return controller.stream;
  }

  Future<void> _startSubscription(
      StreamController<QueueEvent> controller) async {
    try {
      final conn = RedisConnection();
      final cmd = await conn.connect(host, port);
      _subConn = conn;
      final pubsub = PubSub(cmd);
      pubsub.subscribe([channel]);
      pubsub.getStream().listen(
        (msg) {
          // SUBSCRIBE messages arrive as ['message', channel, payload].
          if (msg is List && msg.length == 3 && msg[0] == 'message') {
            try {
              controller.add(QueueEvent.decode(msg[2].toString()));
            } catch (_) {
              // ignore malformed payloads
            }
          }
        },
        onError: controller.addError,
      );
    } catch (e) {
      controller.addError(e);
    }
  }

  Future<Command> _histCommand() async {
    if (_histCmd != null) return _histCmd!;
    final conn = _histConn = RedisConnection();
    return _histCmd = await conn.connect(host, port);
  }

  @override
  Future<List<QueueEvent>> history({int limit = 500}) async {
    try {
      final cmd = await _histCommand();
      final raw = await cmd
          .send_object(<Object>['LRANGE', historyKey, '0', '${limit - 1}']);
      if (raw is! List) return const [];
      // History is newest-first (LPUSH); reverse to oldest-first.
      final out = <QueueEvent>[];
      for (final item in raw.reversed) {
        try {
          out.add(QueueEvent.decode(item.toString()));
        } catch (_) {
          // skip malformed
        }
      }
      return out;
    } catch (_) {
      return const [];
    }
  }

  @override
  Future<void> close() async {
    await _controller?.close();
    await _subConn?.close();
    await _histConn?.close();
    _controller = null;
    _subConn = null;
    _histConn = null;
    _histCmd = null;
  }
}
