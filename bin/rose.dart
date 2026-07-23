import 'dart:io';

import 'package:gisila_queue/gisila_queue.dart';
import 'package:gisila_queue/gisila_rose.dart';

/// Standalone launcher for the GisilaRose dashboard — the gisila_queue
/// analogue of `celery flower`.
///
/// Usage:
///   `dart run gisila_queue:rose [options]`
///
/// Options (all optional):
///   `--redis-host=HOST`   Redis host (default: localhost)
///   `--redis-port=PORT`   Redis port (default: 6379)
///   `--prefix=PREFIX`     Key/channel namespace, must match producers/workers
///                         (default: gq)
///   `--host=HOST`         HTTP bind host (default: 0.0.0.0)
///   `--port=PORT`         HTTP bind port (default: 5555)
///   `--no-queues`         Don't open a broker connection for queue depths
Future<void> main(List<String> argv) async {
  final args = _parse(argv);
  if (args.containsKey('help') || args.containsKey('h')) {
    stdout.writeln(_usage);
    return;
  }

  final redisHost = args['redis-host'] ?? 'localhost';
  final redisPort = int.tryParse(args['redis-port'] ?? '') ?? 6379;
  final prefix = args['prefix'] ?? 'gq';
  final host = args['host'] ?? '0.0.0.0';
  final port = int.tryParse(args['port'] ?? '') ?? 5555;
  final withQueues = !args.containsKey('no-queues');

  final rose = GisilaRose(
    source: RedisEventSource(host: redisHost, port: redisPort, prefix: prefix),
    broker: withQueues
        ? RedisBroker(host: redisHost, port: redisPort, prefix: prefix)
        : null,
    host: host,
    port: port,
  );

  await rose.start();
  stdout.writeln('GisilaRose → http://$host:$port  '
      '(redis $redisHost:$redisPort, prefix "$prefix")');

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

  dart run gisila_queue:rose [--redis-host=H] [--redis-port=P]
      [--prefix=gq] [--host=0.0.0.0] [--port=5555] [--no-queues]
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
