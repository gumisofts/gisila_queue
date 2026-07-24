// ignore_for_file: avoid_print
//
// Throughput benchmark for gisila_queue.
//
// Floods a worker with many lightweight tasks and reports enqueue rate,
// drain rate, end-to-end throughput, and latency percentiles. Defaults to
// the in-memory broker (no Redis/RabbitMQ). Pass `--broker=redis` or
// `--broker=rabbitmq` for a real broker run.
//
// Examples:
//   dart run benchmark/throughput_bench.dart
//   dart run benchmark/throughput_bench.dart --tasks=50000 --concurrency=16
//   dart run benchmark/throughput_bench.dart --broker=redis --tasks=10000
//   dart run benchmark/throughput_bench.dart --broker=rabbitmq --tasks=10000
//   dart run benchmark/throughput_bench.dart --work-us=500 --with-results
//   dart run benchmark/throughput_bench.dart --sweep --tasks=20000

import 'dart:async';
import 'dart:io';
import 'dart:math' as math;

import 'package:dart_amqp/dart_amqp.dart';
import 'package:gisila_queue/gisila_queue.dart';
import 'package:gisila_queue/gisila_queue_cli.dart';
import 'package:redis/redis.dart' as redis;

Future<void> main(List<String> argv) async {
  final args = CliArgs.parse(argv);
  if (args.has('help') || args.has('h')) {
    _printUsage();
    return;
  }

  final base = BenchConfig.fromArgs(args);
  final concurrencies =
      args.has('sweep') ? const [1, 2, 4, 8, 16, 32] : [base.concurrency];

  print('gisila_queue throughput benchmark');
  print('  tasks         : ${base.tasks}');
  print('  concurrency   : ${concurrencies.join(', ')}'
      '${args.has('sweep') ? ' (sweep)' : ''}');
  print('  broker        : ${base.broker}');
  print('  with-results  : ${base.withResults}');
  print('  work-us       : ${base.workUs}');
  print('  enqueue-batch : ${base.enqueueBatch}');
  print('');

  final results = <BenchResult>[];
  for (final concurrency in concurrencies) {
    if (concurrencies.length > 1) {
      print('── concurrency=$concurrency ──');
    }
    final result = await runBenchmark(base.copyWith(concurrency: concurrency));
    results.add(result);
    _printReport(result);
    print('');
  }

  if (results.length > 1) {
    _printSweepSummary(results);
  }
}

class BenchConfig {
  BenchConfig({
    required this.tasks,
    required this.concurrency,
    required this.broker,
    required this.withResults,
    required this.workUs,
    required this.enqueueBatch,
    required this.redisHost,
    required this.redisPort,
    required this.redisPrefix,
    required this.rabbitHost,
    required this.rabbitPort,
    required this.rabbitUser,
    required this.rabbitPassword,
    required this.rabbitVhost,
  });

  factory BenchConfig.fromArgs(CliArgs args) {
    final broker = args.stringOr('broker', 'memory');
    if (broker != 'memory' && broker != 'redis' && broker != 'rabbitmq') {
      stderr.writeln('Unknown --broker=$broker (use memory|redis|rabbitmq)');
      exit(64);
    }
    return BenchConfig(
      tasks: math.max(1, args.intOr('tasks', 10000)),
      concurrency: math.max(1, args.intOr('concurrency', 8)),
      broker: broker,
      withResults: args.has('with-results'),
      workUs: math.max(0, args.intOr('work-us', 0)),
      enqueueBatch: math.max(1, args.intOr('enqueue-batch', 256)),
      redisHost: args.stringOr(
        'redis-host',
        Platform.environment['GISILA_TEST_REDIS_HOST'] ?? 'localhost',
      ),
      redisPort: args.intOr(
        'redis-port',
        int.tryParse(Platform.environment['GISILA_TEST_REDIS_PORT'] ?? '') ??
            6379,
      ),
      redisPrefix: args.stringOr(
        'prefix',
        'gq_bench_${DateTime.now().microsecondsSinceEpoch}',
      ),
      rabbitHost: args.stringOr(
        'rabbitmq-host',
        Platform.environment['GISILA_TEST_RABBITMQ_HOST'] ?? 'localhost',
      ),
      rabbitPort: args.intOr(
        'rabbitmq-port',
        int.tryParse(Platform.environment['GISILA_TEST_RABBITMQ_PORT'] ?? '') ??
            5672,
      ),
      rabbitUser: args.stringOr('rabbitmq-user', 'guest'),
      rabbitPassword: args.stringOr('rabbitmq-password', 'guest'),
      rabbitVhost: args.stringOr('rabbitmq-vhost', '/'),
    );
  }

  final int tasks;
  final int concurrency;
  final String broker;
  final bool withResults;
  final int workUs;
  final int enqueueBatch;
  final String redisHost;
  final int redisPort;
  final String redisPrefix;
  final String rabbitHost;
  final int rabbitPort;
  final String rabbitUser;
  final String rabbitPassword;
  final String rabbitVhost;

  BenchConfig copyWith({int? concurrency}) => BenchConfig(
        tasks: tasks,
        concurrency: concurrency ?? this.concurrency,
        broker: broker,
        withResults: withResults,
        workUs: workUs,
        enqueueBatch: enqueueBatch,
        redisHost: redisHost,
        redisPort: redisPort,
        redisPrefix: redisPrefix,
        rabbitHost: rabbitHost,
        rabbitPort: rabbitPort,
        rabbitUser: rabbitUser,
        rabbitPassword: rabbitPassword,
        rabbitVhost: rabbitVhost,
      );
}

class BenchResult {
  BenchResult({
    required this.tasks,
    required this.concurrency,
    required this.broker,
    required this.withResults,
    required this.workUs,
    required this.enqueueMs,
    required this.drainMs,
    required this.totalMs,
    required this.completed,
    required this.failed,
    required this.latenciesUs,
  });

  final int tasks;
  final int concurrency;
  final String broker;
  final bool withResults;
  final int workUs;
  final int enqueueMs;
  final int drainMs;
  final int totalMs;
  final int completed;
  final int failed;
  final List<int> latenciesUs;

  double get enqueuePerSec =>
      enqueueMs == 0 ? double.infinity : tasks * 1000 / enqueueMs;

  double get drainPerSec =>
      drainMs == 0 ? double.infinity : completed * 1000 / drainMs;

  double get e2ePerSec =>
      totalMs == 0 ? double.infinity : completed * 1000 / totalMs;
}

Broker _buildBroker(BenchConfig config) {
  switch (config.broker) {
    case 'redis':
      return RedisBroker(
        host: config.redisHost,
        port: config.redisPort,
        prefix: config.redisPrefix,
      );
    case 'rabbitmq':
      return RabbitBroker(
        host: config.rabbitHost,
        port: config.rabbitPort,
        virtualHost: config.rabbitVhost,
        username: config.rabbitUser,
        password: config.rabbitPassword,
        prefix: config.redisPrefix,
        prefetchCount: math.max(config.concurrency, 32),
      );
    default:
      return InMemoryBroker(pollInterval: const Duration(milliseconds: 1));
  }
}

Future<BenchResult> runBenchmark(BenchConfig config) async {
  final broker = _buildBroker(config);

  // Result backends: Redis when using the Redis broker; otherwise in-memory
  // (RabbitMQ has no result backend in this package yet).
  final ResultBackend? backend = config.withResults
      ? (config.broker == 'redis'
          ? RedisResultBackend(
              host: config.redisHost,
              port: config.redisPort,
              prefix: '${config.redisPrefix}:results',
            )
          : InMemoryResultBackend())
      : null;

  final app = GisilaQueue(
    broker: broker,
    backend: backend,
    logger: SilentQueueLogger(),
  );

  final completed = Completer<void>();
  var done = 0;
  var failed = 0;
  final latenciesUs = <int>[];
  final sw = Stopwatch();

  // Tiny nop task. Optional synthetic delay via --work-us.
  // Enqueue timestamp travels in kwargs so latency is race-free even when
  // the worker finishes before the producer records local state.
  app.task<int>(
    'bench.nop',
    (ctx) async {
      if (config.workUs > 0) {
        await Future<void>.delayed(Duration(microseconds: config.workUs));
      }
      return ctx.arg(0) as int;
    },
  );

  final worker = app.worker(
    concurrency: config.concurrency,
    // Short reserve so idle slots don't sit on a long BLPOP/poll after drain.
    reserveTimeout: const Duration(milliseconds: 50),
  );
  worker.onSuccess = (message, result) {
    final startUs = message.kwargs['enqueued_us'];
    if (startUs is int) {
      latenciesUs.add(sw.elapsedMicroseconds - startUs);
    }
    done++;
    if (done + failed >= config.tasks && !completed.isCompleted) {
      completed.complete();
    }
  };
  worker.onFailure = (message, result) {
    failed++;
    if (done + failed >= config.tasks && !completed.isCompleted) {
      completed.complete();
    }
  };

  unawaited(worker.run());

  // Warm the consumer loops so the first reserve isn't cold.
  await Future<void>.delayed(const Duration(milliseconds: 20));

  try {
    sw.start();
    final enqueueStart = sw.elapsedMilliseconds;

    for (var offset = 0; offset < config.tasks; offset += config.enqueueBatch) {
      final end = math.min(offset + config.enqueueBatch, config.tasks);
      final batch = <Future<void>>[];
      for (var i = offset; i < end; i++) {
        final enqueuedUs = sw.elapsedMicroseconds;
        batch.add(
          app.send<int>(
            'bench.nop',
            args: [i],
            kwargs: {'enqueued_us': enqueuedUs},
          ).then((_) {}),
        );
      }
      await Future.wait(batch);
    }

    final enqueueMs = sw.elapsedMilliseconds - enqueueStart;
    final drainStart = sw.elapsedMilliseconds;

    await completed.future.timeout(
      Duration(seconds: math.max(30, config.tasks ~/ 50 + 30)),
      onTimeout: () {
        throw TimeoutException(
          'timed out waiting for tasks: completed=$done failed=$failed '
          'expected=${config.tasks}',
        );
      },
    );

    final drainMs = sw.elapsedMilliseconds - drainStart;
    final totalMs = sw.elapsedMilliseconds;

    return BenchResult(
      tasks: config.tasks,
      concurrency: config.concurrency,
      broker: config.broker,
      withResults: config.withResults,
      workUs: config.workUs,
      enqueueMs: enqueueMs,
      drainMs: drainMs,
      totalMs: totalMs,
      completed: done,
      failed: failed,
      latenciesUs: latenciesUs,
    );
  } finally {
    await worker.stop();
    await app.close();
    if (config.broker == 'redis') {
      await _cleanupRedisPrefix(
        config.redisHost,
        config.redisPort,
        config.redisPrefix,
      );
    } else if (config.broker == 'rabbitmq') {
      await _cleanupRabbitPrefix(
        config.rabbitHost,
        config.rabbitPort,
        config.rabbitVhost,
        config.rabbitUser,
        config.rabbitPassword,
        config.redisPrefix,
      );
    }
  }
}

Future<void> _cleanupRedisPrefix(String host, int port, String prefix) async {
  final conn = redis.RedisConnection();
  try {
    final cmd = await conn.connect(host, port);
    var cursor = '0';
    final keys = <String>[];
    do {
      final res =
          await cmd.send_object(<Object>['SCAN', cursor, 'MATCH', '$prefix:*']);
      if (res is! List || res.length != 2) break;
      cursor = res[0].toString();
      final batch = res[1];
      if (batch is List) {
        keys.addAll(batch.map((e) => e.toString()));
      }
    } while (cursor != '0');
    if (keys.isNotEmpty) {
      await cmd.send_object(<Object>['DEL', ...keys]);
    }
  } catch (_) {
    // Best-effort cleanup; ignore if Redis is already gone.
  } finally {
    try {
      await conn.close();
    } catch (_) {}
  }
}

Future<void> _cleanupRabbitPrefix(
  String host,
  int port,
  String vhost,
  String user,
  String password,
  String prefix,
) async {
  final client = Client(
    settings: ConnectionSettings(
      host: host,
      port: port,
      virtualHost: vhost,
      authProvider: PlainAuthenticator(user, password),
      maxConnectionAttempts: 1,
    ),
  );
  try {
    final channel = await client.channel();
    try {
      final q = await channel.queue('$prefix.queue.default', durable: true);
      await q.delete();
    } catch (_) {}
    try {
      final scheduled = await channel.queue(
        '$prefix.scheduled',
        durable: true,
        arguments: <String, Object>{
          'x-dead-letter-exchange': '$prefix.tasks',
        },
      );
      await scheduled.delete();
    } catch (_) {}
    try {
      await (await channel.exchange(
        '$prefix.delay',
        ExchangeType.FANOUT,
        durable: true,
      ))
          .delete();
    } catch (_) {}
    try {
      await (await channel.exchange(
        '$prefix.tasks',
        ExchangeType.DIRECT,
        durable: true,
      ))
          .delete();
    } catch (_) {}
    await channel.close();
  } catch (_) {
    // Best-effort cleanup.
  } finally {
    try {
      await client.close();
    } catch (_) {}
  }
}

void _printReport(BenchResult r) {
  print('── results ──────────────────────────────────────');
  print('  completed      : ${r.completed} / ${r.tasks}'
      '${r.failed == 0 ? '' : '  (failed: ${r.failed})'}');
  print('  enqueue time   : ${_fmtMs(r.enqueueMs)}'
      '  (${_fmtRate(r.enqueuePerSec)} tasks/s)');
  print('  drain time     : ${_fmtMs(r.drainMs)}'
      '  (${_fmtRate(r.drainPerSec)} tasks/s)');
  print('  end-to-end     : ${_fmtMs(r.totalMs)}'
      '  (${_fmtRate(r.e2ePerSec)} tasks/s)');

  if (r.latenciesUs.isNotEmpty) {
    final sorted = List<int>.from(r.latenciesUs)..sort();
    print('  latency p50    : ${_fmtUs(_percentile(sorted, 0.50))}');
    print('  latency p95    : ${_fmtUs(_percentile(sorted, 0.95))}');
    print('  latency p99    : ${_fmtUs(_percentile(sorted, 0.99))}');
    print('  latency max    : ${_fmtUs(sorted.last)}');
  }
  print('─────────────────────────────────────────────────');
}

void _printSweepSummary(List<BenchResult> results) {
  print('── sweep summary ────────────────────────────────');
  print('  conc   e2e tasks/s   drain tasks/s   p99');
  for (final r in results) {
    final sorted = List<int>.from(r.latenciesUs)..sort();
    final p99 = sorted.isEmpty ? '-' : _fmtUs(_percentile(sorted, 0.99));
    print('  ${r.concurrency.toString().padLeft(4)}'
        '   ${_fmtRate(r.e2ePerSec).padLeft(11)}'
        '   ${_fmtRate(r.drainPerSec).padLeft(13)}'
        '   $p99');
  }
  print('─────────────────────────────────────────────────');
}

int _percentile(List<int> sorted, double p) {
  if (sorted.isEmpty) return 0;
  final idx = ((sorted.length - 1) * p).round();
  return sorted[idx];
}

String _fmtMs(int ms) {
  if (ms < 1000) return '${ms}ms';
  return '${(ms / 1000).toStringAsFixed(2)}s';
}

String _fmtUs(int us) {
  if (us < 1000) return '$usµs';
  if (us < 1000000) return '${(us / 1000).toStringAsFixed(1)}ms';
  return '${(us / 1000000).toStringAsFixed(2)}s';
}

String _fmtRate(double rate) {
  if (rate.isInfinite) return '∞';
  if (rate >= 1000) return rate.toStringAsFixed(0);
  return rate.toStringAsFixed(1);
}

void _printUsage() {
  print('''
Usage: dart run benchmark/throughput_bench.dart [options]

Options:
  --tasks=N            Number of tasks to enqueue (default: 10000)
  --concurrency=N      Worker concurrency (default: 8)
  --sweep              Run concurrency sweep: 1,2,4,8,16,32
  --broker=memory|redis|rabbitmq
                       Broker backend (default: memory)
  --with-results       Store results in a result backend
  --work-us=N          Simulated work per task in microseconds (default: 0)
  --enqueue-batch=N    Parallel enqueue batch size (default: 256)
  --redis-host=HOST    Redis host (default: localhost)
  --redis-port=PORT    Redis port (default: 6379)
  --rabbitmq-host=HOST RabbitMQ host (default: localhost)
  --rabbitmq-port=PORT RabbitMQ port (default: 5672)
  --rabbitmq-user=U    RabbitMQ username (default: guest)
  --rabbitmq-password=P
                       RabbitMQ password (default: guest)
  --rabbitmq-vhost=V   RabbitMQ vhost (default: /)
  --prefix=PREFIX      Key/queue prefix (default: unique per run)
  --help               Show this help
''');
}
