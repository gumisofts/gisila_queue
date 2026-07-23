import 'dart:io';

import '../app.dart';
import '../backend/redis_backend.dart';
import '../broker/redis_broker.dart';
import '../control.dart';
import '../logging.dart';
import '../worker/worker.dart';
import 'cli_args.dart';
import 'shutdown.dart';

/// Shared setup for a `bin/worker.dart` entrypoint — parses the standard
/// gisila_queue worker flags, builds a Redis-backed [GisilaQueue], and
/// wires graceful shutdown, so a project's own binary can be little more
/// than:
///
/// ```dart
/// Future<void> main(List<String> argv) async {
///   final cli = WorkerCli.parse(argv);
///   if (cli.help) return print(cli.usage);
///   final app = cli.buildApp();
///   registerMyTasks(app); // your own task registrations
///   await cli.run(app);
/// }
/// ```
///
/// Recognized flags: `--queues=a,b` (default: `default`),
/// `--concurrency=N` (default: 1), `--redis-host=H` (default: localhost),
/// `--redis-port=P` (default: 6379), `--prefix=P` (default: gq),
/// `--loglevel=debug|info|warning|error` (default: info), `--help`/`-h`.
class WorkerCli {
  WorkerCli._(
    this.args, {
    required this.redisHost,
    required this.redisPort,
    required this.prefix,
    required this.queues,
    required this.concurrency,
    required this.logLevel,
  });

  final CliArgs args;
  final String redisHost;
  final int redisPort;
  final String prefix;
  final List<String> queues;
  final int concurrency;
  final LogLevel logLevel;

  factory WorkerCli.parse(List<String> argv) {
    final args = CliArgs.parse(argv);
    return WorkerCli._(
      args,
      redisHost: args.stringOr('redis-host', 'localhost'),
      redisPort: args.intOr('redis-port', 6379),
      prefix: args.stringOr('prefix', 'gq'),
      queues: args.stringList('queues') ?? const ['default'],
      concurrency: args.intOr('concurrency', 1),
      logLevel: _parseLogLevel(args.string('loglevel')),
    );
  }

  bool get help => args.has('help') || args.has('h');

  static const usage = '''
gisila_queue worker CLI

  dart run bin/worker.dart [--queues=default] [--concurrency=1]
      [--redis-host=localhost] [--redis-port=6379] [--prefix=gq]
      [--loglevel=info]
''';

  /// Builds a Redis-backed [GisilaQueue] from the parsed flags. Register
  /// your own tasks on the result before calling [run].
  GisilaQueue buildApp() => GisilaQueue(
        broker: RedisBroker(host: redisHost, port: redisPort, prefix: prefix),
        backend: RedisResultBackend(
            host: redisHost, port: redisPort, prefix: prefix),
        logger: ConsoleQueueLogger(minLevel: logLevel),
        controlChannel: RedisControlChannel(
            host: redisHost, port: redisPort, prefix: prefix),
      );

  /// Builds a [Worker] for [app] from the parsed flags, starts it, prints a
  /// startup banner, and blocks until a `SIGINT`/`SIGTERM` triggers a
  /// graceful [Worker.stop]. Also closes [app]'s broker/backend/control
  /// channel connections before returning, so the process actually exits
  /// instead of hanging on an open Redis socket.
  Future<void> run(GisilaQueue app) async {
    final worker = app.worker(queues: queues, concurrency: concurrency);
    installShutdownHooks(() async {
      stdout.writeln('\nworker ${worker.id} shutting down…');
      await worker.stop();
    });
    stdout.writeln('worker ${worker.id} → queues=${queues.join(',')} '
        'concurrency=$concurrency (redis $redisHost:$redisPort, prefix "$prefix")');
    await worker.run();
    await app.close();
  }
}

LogLevel _parseLogLevel(String? raw) {
  if (raw == null) return LogLevel.info;
  for (final level in LogLevel.values) {
    if (level.name == raw) return level;
  }
  return LogLevel.info;
}
