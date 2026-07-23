import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:collection/collection.dart';

import '../broker/broker.dart';
import '../events/event_sink.dart';
import '../task_state.dart';
import 'dashboard_html.dart';
import 'monitor_state.dart';

/// A Flower-equivalent observability dashboard for gisila_queue.
///
/// It binds a [MonitorState] to an [EventSource] (the live event stream
/// produced by workers/producers) and serves:
///
/// * `GET /` — a self-contained HTML dashboard (auto-refreshing, live tail).
/// * `GET /api/stats` — summary counters, worker/queue rollups.
/// * `GET /api/workers` — per-worker status + counters.
/// * `GET /api/tasks` — recent tasks (filter by `state`/`name`/`queue`,
///   paginate with `limit`/`offset`).
/// * `GET /api/tasks/<id>` — one task's full detail.
/// * `GET /api/queues` — broker depth per known queue.
/// * `GET /api/events` — Server-Sent Events firehose for live updates.
///
/// Run it as its own process (see `bin/rose.dart`) pointed at the same Redis
/// the cluster uses, or embed it in-process with an [InMemoryEventBus].
///
/// ```dart
/// final rose = GisilaRose(
///   source: RedisEventSource(host: 'localhost'),
///   broker: RedisBroker(host: 'localhost'), // optional: enables queue depth
///   port: 5555,
/// );
/// await rose.start();
/// ```
class GisilaRose {
  GisilaRose({
    required this.source,
    this.broker,
    MonitorState? state,
    this.host = '0.0.0.0',
    this.port = 5555,
    this.queuePollInterval = const Duration(seconds: 2),
  }) : state = state ?? MonitorState();

  /// The live event stream the dashboard folds into [state].
  final QueueEventSource source;

  /// Optional broker used purely to poll queue depths. Without it, the
  /// `queues` panel simply stays empty.
  final Broker? broker;

  final MonitorState state;
  final String host;
  final int port;

  /// How often broker queue depths are refreshed.
  final Duration queuePollInterval;

  HttpServer? _server;
  Timer? _queuePoll;
  final Set<HttpResponse> _sseClients = {};

  /// The actual port bound after [start] — useful when [port] was `0`
  /// (bind to any free port), e.g. in tests. Null before [start] / after
  /// [stop].
  int? get boundPort => _server?.port;

  /// Binds the event source, starts queue polling, and begins serving.
  /// Resolves once the server is listening.
  Future<void> start() async {
    await state.bind(source);
    _server = await HttpServer.bind(host, port);
    if (broker != null) {
      _queuePoll = Timer.periodic(queuePollInterval, (_) => _pollQueues());
      unawaited(_pollQueues());
    }
    // Fan live events out to connected SSE clients.
    state.tap.listen((event) {
      if (_sseClients.isEmpty) return;
      final frame = 'data: ${event.encode()}\n\n';
      for (final client in _sseClients.toList()) {
        try {
          client.write(frame);
        } catch (_) {
          _sseClients.remove(client);
        }
      }
    });
    _serve(_server!);
  }

  /// Refreshes [MonitorState.queueLengths] immediately instead of waiting
  /// for the next [queuePollInterval] tick — mainly useful in tests.
  Future<void> pollQueuesNow() => _pollQueues();

  Future<void> _pollQueues() async {
    final b = broker;
    if (b == null) return;
    final names = <String>{
      for (final w in state.workers) ...w.queues,
      ...state.queueLengths.keys,
    };
    for (final name in names) {
      try {
        state.queueLengths[name] = await b.length(name);
      } catch (_) {
        // leave the last known value in place
      }
    }
  }

  void _serve(HttpServer server) {
    server.listen((req) async {
      try {
        await _route(req);
      } catch (e) {
        _json(req.response, {'error': '$e'}, status: 500);
      }
    });
  }

  Future<void> _route(HttpRequest req) async {
    final res = req.response;
    _cors(res);
    if (req.method == 'OPTIONS') {
      res.statusCode = 204;
      await res.close();
      return;
    }
    final path = req.uri.path;

    if (path == '/' || path == '/index.html') {
      res.headers.contentType = ContentType.html;
      res.write(dashboardHtml);
      await res.close();
      return;
    }
    if (path == '/api/stats') {
      _json(res, state.stats(DateTime.now()));
      return;
    }
    if (path == '/api/workers') {
      final now = DateTime.now();
      _json(res, {
        'workers': [
          for (final w in state.workers)
            w.toJson(now: now, timeout: state.workerTimeout),
        ],
      });
      return;
    }
    if (path == '/api/queues') {
      _json(res, {'queues': state.queueLengths});
      return;
    }
    if (path == '/api/tasks') {
      final q = req.uri.queryParameters;
      final tasks = state.tasks(
        state: _parseState(q['state']),
        name: q['name'],
        queue: q['queue'],
        limit: int.tryParse(q['limit'] ?? '') ?? 200,
        offset: int.tryParse(q['offset'] ?? '') ?? 0,
      );
      _json(res, {
        'total': state.taskCount,
        'tasks': [for (final t in tasks) t.toJson()],
      });
      return;
    }
    if (path.startsWith('/api/tasks/')) {
      final id = path.substring('/api/tasks/'.length);
      final t = state.taskById(id);
      if (t == null) {
        _json(res, {'error': 'not found'}, status: 404);
      } else {
        _json(res, t.toJson());
      }
      return;
    }
    if (path == '/api/events') {
      await _streamEvents(res);
      return;
    }

    _json(res, {'error': 'not found'}, status: 404);
  }

  Future<void> _streamEvents(HttpResponse res) async {
    res.statusCode = 200;
    res.headers
      ..set(HttpHeaders.contentTypeHeader, 'text/event-stream')
      ..set(HttpHeaders.cacheControlHeader, 'no-cache')
      ..set(HttpHeaders.connectionHeader, 'keep-alive');
    res.bufferOutput = false;
    res.write(': connected\n\n');
    _sseClients.add(res);
    // Detach: the response stays open until the client disconnects.
    res.done.whenComplete(() => _sseClients.remove(res));
  }

  void _cors(HttpResponse res) {
    res.headers
      ..set('Access-Control-Allow-Origin', '*')
      ..set('Access-Control-Allow-Methods', 'GET, OPTIONS')
      ..set('Access-Control-Allow-Headers', 'Content-Type');
  }

  void _json(HttpResponse res, Object? body, {int status = 200}) {
    res.statusCode = status;
    res.headers.contentType = ContentType.json;
    res.write(jsonEncode(body));
    res.close();
  }

  TaskState? _parseState(String? raw) {
    if (raw == null || raw.isEmpty) return null;
    return TaskState.values.where((s) => s.name == raw).firstOrNull;
  }

  /// Stops serving and releases the event source / broker connections.
  Future<void> stop() async {
    _queuePoll?.cancel();
    _queuePoll = null;
    for (final c in _sseClients.toList()) {
      try {
        await c.close();
      } catch (_) {}
    }
    _sseClients.clear();
    await _server?.close(force: true);
    _server = null;
    await state.close();
    await source.close();
    await broker?.close();
  }
}
