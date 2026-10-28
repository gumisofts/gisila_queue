import 'dart:io';

import 'package:gisila_queue/gisila_queue.dart';
import 'package:gisila_queue/gisila_queue_cli.dart' show CliArgs;
import 'package:gisila_queue/gisila_rose.dart';

/// Standalone launcher for the GisilaRose dashboard — the gisila_queue
/// analogue of `celery flower`.
///
/// Usage:
///   `dart run gisila_queue:rose [options]`
///
/// Options (all optional):
///   `--redis-url=URL`     redis://[user:]pass@host:port/db or rediss://
///                         (TLS); falls back to the REDIS_URL env var
///   `--redis-host=HOST`   Redis host (default: localhost)
///   `--redis-port=PORT`   Redis port (default: 6379)
///   `--namespace=NS`      Key/channel namespace, must match producers/workers
///                         (alias `--prefix`; default: gq)
///   `--host=HOST`         HTTP bind host (default: 127.0.0.1). Any other
///                         address needs --basic-auth while fire/revoke
///                         are enabled
///   `--port=PORT`         HTTP bind port (default: 5555)
///   `--basic-auth=U:P`    Require HTTP Basic Auth (Flower-style)
///   `--no-queues`         Don't open a broker (no depths / fire / retry)
///   `--no-control`        Don't open a control channel (no revoke/pause)
///   `--allow-unauthenticated`  Serve the write actions without auth on a
///                         non-loopback host anyway
Future<void> main(List<String> argv) async {
  final args = _parse(argv);
  if (args.containsKey('help') || args.containsKey('h')) {
    stdout.writeln(_usage);
    return;
  }

  final redis = CliArgs(args).redisTarget();
  final redisHost = redis.host;
  final redisPort = redis.port;
  final redisOptions = redis.options;
  final prefix = args['namespace'] ?? args['prefix'] ?? 'gq';
  final host = args['host'] ?? '127.0.0.1';
  final port = int.tryParse(args['port'] ?? '') ?? 5555;
  final withQueues = !args.containsKey('no-queues');
  final withControl = !args.containsKey('no-control');
  final basicAuth = args['basic-auth'];

  if (basicAuth != null && !basicAuth.contains(':')) {
    stderr.writeln('error: --basic-auth must be user:password');
    exitCode = 64;
    return;
  }

  final rose = GisilaRose(
    source: RedisEventSource(
        host: redisHost,
        port: redisPort,
        options: redisOptions,
        prefix: prefix),
    broker: withQueues
        ? RedisBroker(
            host: redisHost,
            port: redisPort,
            options: redisOptions,
            prefix: prefix)
        : null,
    control: withControl
        ? RedisControlChannel(
            host: redisHost,
            port: redisPort,
            options: redisOptions,
            prefix: prefix)
        : null,
    events: RedisEventSink(
        host: redisHost,
        port: redisPort,
        options: redisOptions,
        prefix: prefix),
    backend: withQueues
        ? RedisResultBackend(
            host: redisHost,
            port: redisPort,
            options: redisOptions,
            prefix: prefix)
        : null,
    basicAuth: basicAuth,
    allowUnauthenticated: args.containsKey('allow-unauthenticated'),
    host: host,
    port: port,
  );

  try {
    await rose.start();
  } on StateError catch (e) {
    stderr.writeln('error: ${e.message}');
    exitCode = 64;
    return;
  }
  final authNote = basicAuth != null ? ', basic-auth on' : '';
  stdout.writeln('GisilaRose → http://$host:$port  '
      '(redis $redisHost:$redisPort, namespace "$prefix"$authNote)');

  // Shut down cleanly on Ctrl-C / SIGTERM.
  Future<void> shutdown(ProcessSignal _) async {
    stdout.writeln('\nshutting down…');
    await rose.stop();
    exit(0);
  }

  ProcessSignal.sigint.watch().listen(shutdown);
  try {
    ProcessSignal.sigterm.watch().listen(shutdown);
  } catch (_) {
    // SIGTERM isn't available on Windows; ignore.
  }
}

const _usage = '''
gisila_queue:rose — observability dashboard for gisila_queue

  dart run gisila_queue:rose [--redis-url=URL | --redis-host=H --redis-port=P]
      [--namespace=gq] [--host=127.0.0.1] [--port=5555]
      [--basic-auth=user:pass] [--no-queues] [--no-control]
      [--allow-unauthenticated]

  Binding anything but loopback needs --basic-auth while fire/retry/revoke
  are available (or --no-queues --no-control, or --allow-unauthenticated).
''';

Map<String, String> _parse(List<String> argv) {
  final out = <String, String>{};
  for (final a in argv) {
    if (!a.startsWith('--')) continue;
    final body = a.substring(2);
    final eq = body.indexOf('=');
    if (eq == -1) {
      out[body] = 'true';
    } else {
      out[body.substring(0, eq)] = body.substring(eq + 1);
    }
  }
  return out;
}
