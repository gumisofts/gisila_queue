import 'dart:async';

import 'package:redis/redis.dart';

import '../redis_options.dart';

import 'result_backend.dart';

/// Redis-backed [ResultBackend]. Each result is a string key at
/// `<prefix>:result:<taskId>` carrying the JSON-encoded [StoredResult],
/// expiring after [resultTtl] so completed results don't accumulate.
///
/// The connection reconnects transparently and retries once if a call fails
/// (e.g. after a Redis restart), instead of permanently wedging for the
/// lifetime of this instance.
class RedisResultBackend extends ResultBackend {
  RedisResultBackend({
    this.host = 'localhost',
    this.port = 6379,
    this.options = const RedisOptions(),
    this.prefix = 'gq',
    this.resultTtl = const Duration(hours: 24),
  });

  final String host;
  final int port;

  /// Password, TLS and database; see [RedisOptions].
  final RedisOptions options;
  final String prefix;

  /// How long a stored result survives before Redis evicts it.
  final Duration resultTtl;

  RedisConnection? _conn;
  Command? _cmd;
  Future<Command>? _connecting;

  String _key(String taskId) => '$prefix:result:$taskId';

  Future<Command> _command() {
    final cached = _cmd;
    if (cached != null) return Future.value(cached);
    return _connecting ??= _doConnect();
  }

  Future<Command> _doConnect() async {
    try {
      final conn = RedisConnection();
      final cmd = await connectRedis(conn, host, port, options);
      _conn = conn;
      _cmd = cmd;
      return cmd;
    } finally {
      _connecting = null;
    }
  }

  void _reset() {
    final conn = _conn;
    _cmd = null;
    _connecting = null;
    _conn = null;
    unawaited(conn?.close());
  }

  Future<T> _withCommand<T>(Future<T> Function(Command) body) async {
    final cmd = await _command();
    try {
      return await body(cmd);
    } catch (_) {
      _reset();
      final retryCmd = await _command();
      return await body(retryCmd);
    }
  }

  @override
  Future<void> store(String taskId, StoredResult result) =>
      _withCommand((cmd) async {
        await cmd.send_object(<Object>[
          'SET',
          _key(taskId),
          result.encode(),
          'PX',
          '${resultTtl.inMilliseconds}',
        ]);
      });

  @override
  Future<StoredResult?> fetch(String taskId) => _withCommand((cmd) async {
        final raw = await cmd.send_object(<Object>['GET', _key(taskId)]);
        if (raw == null) return null;
        return StoredResult.decode(raw.toString());
      });

  @override
  Future<void> forget(String taskId) => _withCommand((cmd) async {
        await cmd.send_object(<Object>['DEL', _key(taskId)]);
      });

  String _chordCountKey(String chordId) => '$prefix:chord:$chordId:count';
  String _chordAbortKey(String chordId) => '$prefix:chord:$chordId:aborted';

  @override
  Future<bool> chordArrive(String chordId, int total) =>
      _withCommand((cmd) async {
        final raw =
            await cmd.send_object(<Object>['INCR', _chordCountKey(chordId)]);
        final n = raw is int ? raw : int.tryParse('$raw') ?? 0;
        if (n < total) {
          // Safety-net TTL so an abandoned chord (a member that never
          // finishes) doesn't leak its counter key forever.
          await cmd.send_object(<Object>[
            'EXPIRE',
            _chordCountKey(chordId),
            '${resultTtl.inSeconds}'
          ]);
          return false;
        }
        await cmd.send_object(<Object>['DEL', _chordCountKey(chordId)]);
        final aborted =
            await cmd.send_object(<Object>['GET', _chordAbortKey(chordId)]);
        return aborted == null;
      });

  @override
  Future<bool> chordAbort(String chordId) => _withCommand((cmd) async {
        final result = await cmd.send_object(<Object>[
          'SET',
          _chordAbortKey(chordId),
          '1',
          'NX',
          'EX',
          '${resultTtl.inSeconds}',
        ]);
        if (result != null && '$result'.toUpperCase() == 'OK') {
          await cmd.send_object(<Object>['DEL', _chordCountKey(chordId)]);
          return true;
        }
        return false;
      });

  @override
  Future<void> close() async {
    await _conn?.close();
    _conn = null;
    _cmd = null;
    _connecting = null;
  }
}
