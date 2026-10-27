import 'dart:async';

import 'package:redis/redis.dart';

import '../redis_options.dart';

/// A distributed mutex a [Beat] holds while it's the active scheduler.
///
/// Celery documents that exactly one `celery beat` process must run per
/// cluster but otherwise leaves enforcing that up to you; [BeatLock] makes
/// it enforceable — accidentally starting a second beat process no longer
/// double-fires every periodic task, it just sits idle until the first one
/// goes away.
abstract class BeatLock {
  /// Attempts to become (or remain) the leader under [owner], holding the
  /// lock for [ttl]. Returns true iff [owner] holds the lock afterwards.
  Future<bool> acquire(String owner, Duration ttl);

  /// Releases the lock if [owner] currently holds it. Best-effort; letting
  /// the TTL lapse is always sufficient on its own.
  Future<void> release(String owner);

  Future<void> close() async {}
}

/// The default [BeatLock]: always "acquires" successfully. Preserves
/// today's single-instance-by-convention behaviour with zero overhead —
/// use [RedisBeatLock] when more than one process might run `Beat`.
class NoopBeatLock implements BeatLock {
  const NoopBeatLock();

  @override
  Future<bool> acquire(String owner, Duration ttl) async => true;

  @override
  Future<void> release(String owner) async {}

  @override
  Future<void> close() async {}
}

/// A Redis-backed [BeatLock] using a single TTL'd key
/// (`SET key owner NX PX ttl`), renewed by re-acquiring on every call.
///
/// This is intentionally simple rather than Redlock-grade: renewal is a
/// read-then-write (`GET` compare, then `PEXPIRE`) rather than a single
/// atomic script, so there's a narrow race if the key expires at the exact
/// instant of renewal. That's an acceptable trade-off for deduplicating
/// periodic-task firing (the failure mode is "briefly no leader" or, in the
/// rarest case, "two leaders for one tick"), not a correctness-critical
/// distributed lock.
class RedisBeatLock implements BeatLock {
  RedisBeatLock({
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

  String get _key => '$prefix:beat:lock';

  RedisConnection? _conn;
  Command? _cmd;
  Future<Command>? _connecting;

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
  Future<bool> acquire(String owner, Duration ttl) => _withCommand((cmd) async {
        final ms = '${ttl.inMilliseconds}';
        final result =
            await cmd.send_object(<Object>['SET', _key, owner, 'NX', 'PX', ms]);
        if (result != null && '$result'.toUpperCase() == 'OK') return true;
        // Not a fresh acquire: extend the TTL if we still own it. Checked and
        // extended in one script, so a lock that expired and was taken by
        // another instance between the two steps is never extended.
        final renewed = await cmd
            .send_object(<Object>['EVAL', _renewScript, '1', _key, owner, ms]);
        return '$renewed' == '1';
      });

  @override
  Future<void> release(String owner) => _withCommand((cmd) async {
        // Delete only our own lock, atomically (see acquire).
        await cmd
            .send_object(<Object>['EVAL', _releaseScript, '1', _key, owner]);
      });

  static const _renewScript = '''
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0''';

  static const _releaseScript = '''
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0''';

  @override
  Future<void> close() async {
    await _conn?.close();
    _conn = null;
    _cmd = null;
    _connecting = null;
  }
}

/// Where [Beat] persists each entry's last-fired time, so a restart doesn't
/// forget it and immediately refire every [Schedule.every] entry.
abstract class BeatStateStore {
  Future<DateTime?> getLastRun(String entryName);
  Future<void> setLastRun(String entryName, DateTime at);
  Future<void> close() async {}
}

/// The default [BeatStateStore]: keeps nothing — a restart forgets
/// last-run times, matching today's behaviour. Fine for single-process/dev
/// use; pass a [RedisBeatStateStore] for anything long-lived.
class NoopBeatStateStore implements BeatStateStore {
  const NoopBeatStateStore();

  @override
  Future<DateTime?> getLastRun(String entryName) async => null;

  @override
  Future<void> setLastRun(String entryName, DateTime at) async {}

  @override
  Future<void> close() async {}
}

/// Persists last-run times in a Redis hash (`<prefix>:beat:lastrun`),
/// surviving a beat process restart.
class RedisBeatStateStore implements BeatStateStore {
  RedisBeatStateStore({
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

  String get _key => '$prefix:beat:lastrun';

  RedisConnection? _conn;
  Command? _cmd;
  Future<Command>? _connecting;

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
  Future<DateTime?> getLastRun(String entryName) => _withCommand((cmd) async {
        final raw = await cmd.send_object(<Object>['HGET', _key, entryName]);
        if (raw == null) return null;
        return DateTime.tryParse(raw.toString())?.toLocal();
      });

  @override
  Future<void> setLastRun(String entryName, DateTime at) =>
      _withCommand((cmd) async {
        await cmd.send_object(<Object>[
          'HSET',
          _key,
          entryName,
          at.toUtc().toIso8601String(),
        ]);
      });

  @override
  Future<void> close() async {
    await _conn?.close();
    _conn = null;
    _cmd = null;
    _connecting = null;
  }
}
