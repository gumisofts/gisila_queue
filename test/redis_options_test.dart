import 'dart:io';

import 'package:gisila_queue/gisila_queue.dart';
import 'package:gisila_queue/gisila_queue_cli.dart';
import 'package:test/test.dart';

void main() {
  group('RedisOptions.parseUrl', () {
    test('password-only, user and password, TLS and database', () {
      final a = RedisOptions.parseUrl('redis://:s%40cret@cache:6380/2');
      expect((a.host, a.port), ('cache', 6380));
      expect(a.options.username, isNull);
      expect(a.options.password, 's@cret');
      expect(a.options.database, 2);
      expect(a.options.tls, isFalse);

      final b = RedisOptions.parseUrl('rediss://app:pw@redis.example');
      expect((b.host, b.port), ('redis.example', 6379));
      expect(b.options.username, 'app');
      expect(b.options.password, 'pw');
      expect(b.options.tls, isTrue);

      final c = RedisOptions.parseUrl('redis://onlypass@h');
      expect(c.options.password, 'onlypass');
      expect(c.options.username, isNull);
    });

    test('rejects other schemes and bad databases', () {
      expect(() => RedisOptions.parseUrl('http://h'), throwsFormatException);
      expect(() => RedisOptions.parseUrl('redis://h/x'), throwsFormatException);
    });

    test('CLIs prefer --redis-url over host and port', () {
      final t = CliArgs.parse(['--redis-url=redis://:p@x:1', '--redis-host=y'])
          .redisTarget();
      expect((t.host, t.port, t.options.password), ('x', 1, 'p'));
      final plain = CliArgs.parse(['--redis-host=y', '--redis-port=2']);
      if (Platform.environment['REDIS_URL'] == null) {
        final p = plain.redisTarget();
        expect((p.host, p.port, p.options.password), ('y', 2, null));
      }
    });
  });

  // Needs a Redis started with `--requirepass`; set GISILA_TEST_REDIS_AUTH_URL
  // to its URL (e.g. redis://:pw@localhost:6393).
  final authUrl = Platform.environment['GISILA_TEST_REDIS_AUTH_URL'];
  group('password-protected Redis', () {
    test('connects with the password and fails without it', () async {
      final t = RedisOptions.parseUrl(authUrl!);
      final broker = RedisBroker(
          host: t.host, port: t.port, options: t.options, prefix: 'gq-auth');
      expect(await broker.length('default'), isA<int>());
      await broker.close();

      final noAuth = RedisBroker(host: t.host, port: t.port, prefix: 'gq-auth');
      await expectLater(noAuth.length('default'), throwsA(anything));
      await noAuth.close();

      final wrong = RedisBroker(
          host: t.host,
          port: t.port,
          options: const RedisOptions(password: 'wrong'),
          prefix: 'gq-auth');
      await expectLater(wrong.length('default'), throwsA(anything));
      await wrong.close();
    });
  },
      tags: ['redis'],
      skip: authUrl == null ? 'set GISILA_TEST_REDIS_AUTH_URL' : false);
}
