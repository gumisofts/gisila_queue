import 'dart:async';
import 'dart:convert';

import 'package:redis/redis.dart';

import 'redis_options.dart';

/// The kind of instruction a [ControlChannel] carries between producers and
/// workers — the gisila_queue analogue of Celery's `app.control` broadcast
/// commands (`revoke`, and the worker pool's `pause`/`resume` consumption).
enum ControlMessageType { revoke, pause, resume }

/// A single instruction published on a [ControlChannel]. JSON-encodable so
/// it can travel over Redis pub/sub.
class ControlMessage {
  const ControlMessage.revoke(this.taskId, {this.terminate = false})
      : type = ControlMessageType.revoke,
        queue = null;

  const ControlMessage.pause(this.queue)
      : type = ControlMessageType.pause,
        taskId = null,
        terminate = false;

  const ControlMessage.resume(this.queue)
      : type = ControlMessageType.resume,
        taskId = null,
        terminate = false;

  final ControlMessageType type;
  final String? taskId;
  final String? queue;

  /// Requested alongside [ControlMessageType.revoke]. **Not currently
  /// enforced** — a worker cannot forcibly stop a handler already running
  /// within its isolate (see [Worker]'s docs on cooperative concurrency).
  /// Kept on the wire so a future isolate-per-task execution mode can
  /// honour it without a protocol change; today it only prevents the task
  /// from being *started* if it hasn't been yet.
  final bool terminate;

  Map<String, Object?> toJson() => {
        'type': type.name,
        if (taskId != null) 'task_id': taskId,
        if (queue != null) 'queue': queue,
        if (terminate) 'terminate': true,
      };

  factory ControlMessage.fromJson(Map<String, Object?> json) {
    switch (json['type']) {
      case 'revoke':
        return ControlMessage.revoke(json['task_id'] as String,
            terminate: json['terminate'] == true);
      case 'pause':
        return ControlMessage.pause(json['queue'] as String);
      case 'resume':
        return ControlMessage.resume(json['queue'] as String);
      default:
        throw FormatException('unknown control message type "${json['type']}"');
    }
  }

  String encode() => jsonEncode(toJson());

  factory ControlMessage.decode(String raw) =>
      ControlMessage.fromJson(jsonDecode(raw) as Map<String, Object?>);

  @override
  String toString() => 'ControlMessage(${type.name}'
      '${taskId != null ? ' task=$taskId' : ''}'
      '${queue != null ? ' queue=$queue' : ''})';
}

/// A broadcast channel producers use to reach every running worker —
/// Celery's `app.control`. [GisilaQueue] defaults to a [NoopControlChannel]
/// (zero overhead); pass an [InMemoryControlChannel] for a single isolate
/// or a [RedisControlChannel] for a cluster to enable [AsyncResult.revoke]
/// and [Worker] queue pause/resume.
abstract class ControlChannel {
  /// Publishes [message] to every current subscriber.
  Future<void> publish(ControlMessage message);

  /// A broadcast stream of every message published from now on. There is
  /// no history/backfill — a subscriber only sees messages published after
  /// it starts listening (matching Celery's own broadcast control, which is
  /// also not persisted).
  Stream<ControlMessage> subscribe();

  Future<void> close() async {}
}

/// Discards every message and never delivers any. The zero-overhead
/// default — matches today's behaviour where control messages don't exist.
class NoopControlChannel implements ControlChannel {
  const NoopControlChannel();

  @override
  Future<void> publish(ControlMessage message) async {}

  @override
  Stream<ControlMessage> subscribe() => const Stream.empty();

  @override
  Future<void> close() async {}
}

/// In-process [ControlChannel] backed by a broadcast stream. Producers and
/// workers must share the same instance and live in the same isolate.
class InMemoryControlChannel implements ControlChannel {
  final StreamController<ControlMessage> _controller =
      StreamController<ControlMessage>.broadcast();

  @override
  Future<void> publish(ControlMessage message) async {
    if (_controller.hasListener) _controller.add(message);
  }

  @override
  Stream<ControlMessage> subscribe() => _controller.stream;

  @override
  Future<void> close() async {
    await _controller.close();
  }
}

/// Distributed [ControlChannel] over Redis pub/sub (channel
/// `<prefix>:control`), reaching every worker process subscribed at the
/// time a message is published.
class RedisControlChannel implements ControlChannel {
  RedisControlChannel({
    this.host = 'localhost',
    this.port = 6379,
    this.options = const RedisOptions(),
    this.prefix = 'gq',
  });

  final String host;
  final int port;

  /// Password, TLS and database; see [RedisOptions].
  final RedisOptions options;
  final String prefix;

  String get _channel => '$prefix:control';

  RedisConnection? _pubConn;
  Command? _pub;
  Future<Command>? _pubConnecting;

  Future<Command> _pubCmd() {
    final cached = _pub;
    if (cached != null) return Future.value(cached);
    return _pubConnecting ??= _doConnectPub();
  }

  Future<Command> _doConnectPub() async {
    try {
      final conn = RedisConnection();
      final cmd = await connectRedis(conn, host, port, options);
      _pubConn = conn;
      _pub = cmd;
      return cmd;
    } finally {
      _pubConnecting = null;
    }
  }

  void _resetPub() {
    final conn = _pubConn;
    _pub = null;
    _pubConnecting = null;
    _pubConn = null;
    unawaited(conn?.close());
  }

  @override
  Future<void> publish(ControlMessage message) async {
    final cmd = await _pubCmd();
    try {
      await cmd.send_object(<Object>['PUBLISH', _channel, message.encode()]);
    } catch (_) {
      _resetPub();
      final retryCmd = await _pubCmd();
      await retryCmd
          .send_object(<Object>['PUBLISH', _channel, message.encode()]);
    }
  }

  RedisConnection? _subConn;
  StreamController<ControlMessage>? _subController;
  StreamSubscription<Object?>? _pubsubSub;

  @override
  Stream<ControlMessage> subscribe() {
    final existing = _subController;
    if (existing != null) return existing.stream;
    final controller =
        StreamController<ControlMessage>.broadcast(onCancel: () {});
    _subController = controller;
    _startSubscription(controller);
    return controller.stream;
  }

  Future<void> _startSubscription(
      StreamController<ControlMessage> controller) async {
    try {
      final conn = RedisConnection();
      final cmd = await connectRedis(conn, host, port, options);
      _subConn = conn;
      final pubsub = PubSub(cmd);
      pubsub.subscribe([_channel]);
      _pubsubSub = pubsub.getStream().listen(
        (msg) {
          if (controller.isClosed) return;
          if (msg is List && msg.length == 3 && msg[0] == 'message') {
            try {
              controller.add(ControlMessage.decode(msg[2].toString()));
            } catch (_) {
              // ignore malformed control payloads
            }
          }
        },
        onError: (Object e, StackTrace st) {
          if (!controller.isClosed) controller.addError(e, st);
        },
      );
    } catch (e) {
      if (!controller.isClosed) controller.addError(e);
    }
  }

  @override
  Future<void> close() async {
    await _pubsubSub?.cancel();
    await _subController?.close();
    await _subConn?.close();
    await _pubConn?.close();
    _pubsubSub = null;
    _subController = null;
    _subConn = null;
    _pub = null;
    _pubConn = null;
    _pubConnecting = null;
  }
}
