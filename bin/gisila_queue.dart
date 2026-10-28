import 'dart:io';

import 'package:gisila_queue/gisila_queue.dart';
import 'package:gisila_queue/gisila_queue_cli.dart';

/// A small `celery inspect`/`celery purge` analogue for gisila_queue.
///
/// Usage:
///   `dart run gisila_queue:gisila_queue inspect [--queues=a,b] [options]`
///   `dart run gisila_queue:gisila_queue purge --queue=NAME [options]`
///
/// Options (all optional): `--redis-url=URL` (or `REDIS_URL`; see
/// [RedisOptions.parseUrl]), `--redis-host=HOST` (default: localhost),
/// `--redis-port=PORT` (default: 6379), `--namespace=NS` (alias `--prefix`; default: gq).
Future<void> main(List<String> argv) async {
  if (argv.isEmpty || argv.first == '--help' || argv.first == '-h') {
    stdout.writeln(_usage);
    exitCode = argv.isEmpty ? 64 : 0;
    return;
  }

  final command = argv.first;
  final args = CliArgs.parse(argv.skip(1).toList());
  final redis = args.redisTarget();
  final host = redis.host;
  final port = redis.port;
  final prefix = args.string('namespace') ?? args.stringOr('prefix', 'gq');
  final broker = RedisBroker(
      host: host, port: port, options: redis.options, prefix: prefix);

  try {
    switch (command) {
      case 'inspect':
        final queues = args.stringList('queues') ?? const ['default'];
        stdout.writeln(
            'gisila_queue inspect — redis $host:$port, namespace "$prefix"');
        for (final queue in queues) {
          final n = await broker.length(queue);
          stdout.writeln('  $queue: $n ready message(s)');
        }
        final unacked = await broker.unackedCount();
        stdout.writeln('  (in-flight / unacked across all queues: $unacked)');
      case 'purge':
        final queue = args.string('queue');
        if (queue == null) {
          stderr.writeln('purge requires --queue=NAME');
          exitCode = 64;
          return;
        }
        final n = await broker.purge(queue);
        stdout.writeln('purged $n message(s) from "$queue"');
      default:
        stderr.writeln('unknown command "$command"\n');
        stdout.writeln(_usage);
        exitCode = 64;
    }
  } finally {
    await broker.close();
  }
}

const _usage = '''
gisila_queue — inspect/purge utility

  dart run gisila_queue:gisila_queue inspect [--queues=default,emails]
      [--redis-url=URL | --redis-host=H --redis-port=P] [--namespace=gq]
  dart run gisila_queue:gisila_queue purge --queue=NAME
      [--redis-url=URL | --redis-host=H --redis-port=P] [--namespace=gq]

  --redis-url falls back to the REDIS_URL environment variable.
''';
