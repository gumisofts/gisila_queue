import 'dart:io';

import 'package:redis/redis.dart';

/// How to authenticate to and talk to Redis, beyond host and port.
///
/// Every Redis-backed component ([RedisBroker], [RedisResultBackend],
/// [RedisEventSink], [RedisEventSource], [RedisControlChannel],
/// [RedisBeatLock], [RedisBeatStateStore]) takes one as `options:`.
///
/// ```dart
/// final url = RedisOptions.parseUrl(Platform.environment['REDIS_URL']!);
/// final broker = RedisBroker(host: url.host, port: url.port,
///     options: url.options);
/// ```
class RedisOptions {
  const RedisOptions({
    this.username,
    this.password,
    this.tls = false,
    this.database = 0,
    this.securityContext,
  });

  /// ACL user (Redis 6+). Sent only together with [password].
  final String? username;

  /// Sent with `AUTH` right after connecting.
  final String? password;

  /// Connect over TLS (`rediss://`).
  final bool tls;

  /// Logical database, chosen with `SELECT` when not `0`.
  final int database;

  /// Trust and client certificates for [tls]; the system roots otherwise.
  final SecurityContext? securityContext;

  /// Parses `redis://[user[:password]@]host[:port][/db]` (or `rediss://`
  /// for TLS). A URL with only one credential treats it as the password,
  /// the way `redis-cli -u` does.
  static ({String host, int port, RedisOptions options}) parseUrl(
    String url, {
    SecurityContext? securityContext,
  }) {
    final uri = Uri.parse(url);
    if (uri.scheme != 'redis' && uri.scheme != 'rediss') {
      throw FormatException('Not a redis:// or rediss:// URL', url);
    }
    String? username;
    String? password;
    if (uri.userInfo.isNotEmpty) {
      final i = uri.userInfo.indexOf(':');
      if (i < 0) {
        password = Uri.decodeComponent(uri.userInfo);
      } else {
        final user = Uri.decodeComponent(uri.userInfo.substring(0, i));
        username = user.isEmpty ? null : user;
        password = Uri.decodeComponent(uri.userInfo.substring(i + 1));
      }
    }
    final db = uri.pathSegments.isEmpty || uri.pathSegments.first.isEmpty
        ? 0
        : int.tryParse(uri.pathSegments.first) ??
            (throw FormatException('Bad database number in Redis URL', url));
    return (
      host: uri.host.isEmpty ? 'localhost' : uri.host,
      port: uri.hasPort ? uri.port : 6379,
      options: RedisOptions(
        username: username,
        password: password,
        tls: uri.scheme == 'rediss',
        database: db,
        securityContext: securityContext,
      ),
    );
  }
}

/// Opens [conn] to [host]:[port] and applies [options] (TLS, AUTH, SELECT).
Future<Command> connectRedis(
  RedisConnection conn,
  String host,
  int port,
  RedisOptions options,
) async {
  final Command cmd;
  if (options.tls) {
    final socket = await SecureSocket.connect(host, port,
        context: options.securityContext);
    cmd = await conn.connectWithSocket(socket);
  } else {
    cmd = await conn.connect(host, port);
  }
  try {
    final password = options.password;
    if (password != null) {
      await cmd.send_object([
        'AUTH',
        if (options.username != null) options.username!,
        password,
      ]);
    }
    if (options.database != 0) {
      await cmd.send_object(['SELECT', '${options.database}']);
    }
  } catch (_) {
    await conn.close();
    rethrow;
  }
  return cmd;
}
