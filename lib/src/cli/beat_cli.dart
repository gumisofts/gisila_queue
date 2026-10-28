import 'dart:io';

import '../app.dart';
import '../backend/redis_backend.dart';
import '../beat/beat.dart';
import '../beat/coordination.dart';
import '../broker/redis_broker.dart';
import '../logging.dart';
import '../namespace.dart';
import '../redis_options.dart';
import 'cli_args.dart';
import 'shutdown.dart';

/// Shared setup for a `bin/beat.dart` entrypoint — parses the standard
/// gisila_queue beat flags, builds a Redis-backed [GisilaQueue] and a
/// [Beat] wired with a [RedisBeatLock] + [RedisBeatStateStore] (so running
/// two by accident doesn't double-fire everything), and wires graceful
/// shutdown:
///
/// ```dart
/// Future<void> main(List<String> argv) async {
///   final cli = BeatCli.parse(argv);
///   if (cli.help) return print(cli.usage);
///   final app = cli.buildApp();
///   final beat = cli.buildBeat(app)
///     ..schedule('cleanup', 'tasks.cleanup', Schedule.every(Duration(minutes: 5)));
///   await cli.run(beat);
/// }
/// ```
///
/// Recognized flags: `--redis-host=H` (default: localhost),
/// `--redis-port=P` (default: 6379), `--namespace=P` (alias `--prefix`; default: gq),
/// `--tick-interval=SECONDS` (default: 1), `--lock-ttl=SECONDS`
/// (default: 30), `--loglevel=debug|info|warning|error` (default: info),
/// `--help`/`-h`.
class BeatCli {
  BeatCli._(
    this.args, {
    required this.redisHost,
    required this.redisPort,
    required this.redisOptions,
    required this.prefix,
    required this.tickInterval,
    required this.lockTtl,
    required this.logLevel,
  });

  final CliArgs args;
  final String redisHost;
  final int redisPort;

  /// Password, TLS and database from `--redis-url` / `REDIS_URL`.
  final RedisOptions redisOptions;
  final String prefix;
  final Duration tickInterval;
  final Duration lockTtl;
  final LogLevel logLevel;

  factory BeatCli.parse(List<String> argv) {
    final args = CliArgs.parse(argv);
    final redis = args.redisTarget();
    return BeatCli._(
      args,
      redisHost: redis.host,
      redisPort: redis.port,
      redisOptions: redis.options,
      prefix:
          args.string('namespace') ?? args.stringOr('prefix', defaultNamespace),
      tickInterval: Duration(seconds: args.intOr('tick-interval', 1)),
      lockTtl: Duration(seconds: args.intOr('lock-ttl', 30)),
      logLevel: _parseLogLevel(args.string('loglevel')),
    );
  }

  bool get help => args.has('help') || args.has('h');

  static const usage = '''
gisila_queue beat CLI

  dart run bin/beat.dart [--redis-url=redis://:PASS@host:6379/0 |
      --redis-host=localhost --redis-port=6379]
      [--namespace=gq] [--tick-interval=1] [--lock-ttl=30] [--loglevel=info]
''';

  /// Builds a Redis-backed [GisilaQueue] from the parsed flags. Only the
  /// broker is actually used by [Beat] (it just enqueues); a backend is
  /// included anyway so any tasks defined on the same app can be sent
  /// synchronously for testing without a separate producer process.
  GisilaQueue buildApp() => GisilaQueue(
        broker: RedisBroker(
            host: redisHost,
            port: redisPort,
            options: redisOptions,
            prefix: prefix),
        backend: RedisResultBackend(
            host: redisHost,
            port: redisPort,
            options: redisOptions,
            prefix: prefix),
        logger: ConsoleQueueLogger(minLevel: logLevel),
      );

  /// Builds a [Beat] for [app], wired with a Redis leader lock and
  /// persisted last-run state so a second beat process started by accident
  /// sits idle instead of double-firing every entry. Register your
  /// periodic entries on the result before calling [run].
  Beat buildBeat(GisilaQueue app) => Beat(
        app,
        tickInterval: tickInterval,
        lockTtl: lockTtl,
        lock: RedisBeatLock(
            host: redisHost,
            port: redisPort,
            options: redisOptions,
            prefix: prefix),
        stateStore: RedisBeatStateStore(
            host: redisHost,
            port: redisPort,
            options: redisOptions,
            prefix: prefix),
      );

  /// Starts [beat], prints a startup banner, and blocks until a
  /// `SIGINT`/`SIGTERM` triggers a graceful [Beat.stop]. Also closes
  /// [beat]'s app connections before returning, so the process actually
  /// exits instead of hanging on an open Redis socket ([Beat.stop] already
  /// closes the lock/state-store connections it owns).
  Future<void> run(Beat beat) async {
    installShutdownHooks(() async {
      stdout.writeln('\nbeat ${beat.id} shutting down…');
      await beat.stop();
    });
    stdout.writeln('beat ${beat.id} → tick=${tickInterval.inSeconds}s '
        '(redis $redisHost:$redisPort, namespace "$prefix")');
    await beat.run();
    await beat.app.close();
  }
}

LogLevel _parseLogLevel(String? raw) {
  if (raw == null) return LogLevel.info;
  for (final level in LogLevel.values) {
    if (level.name == raw) return level;
  }
  return LogLevel.info;
}
