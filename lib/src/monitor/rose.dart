import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:collection/collection.dart';
import 'package:uuid/uuid.dart';

import '../backend/result_backend.dart';
import '../broker/broker.dart';
import '../codec.dart';
import '../control.dart';
import '../events/event.dart';
import '../events/event_sink.dart';
import '../message.dart';
import '../task_state.dart';
import 'dashboard_html.dart';
import 'monitor_state.dart';

/// A Flower-equivalent observability (and control) dashboard for gisila_queue.
///
/// It binds a [MonitorState] to an [EventSource] (the live event stream
/// produced by workers/producers) and serves:
///
/// * `GET /` — a self-contained HTML dashboard (auto-refreshing, live tail).
/// * `GET /api/stats` — summary counters, worker/queue rollups, capabilities.
/// * `GET /api/workers` — per-worker status + counters + active task ids.
/// * `GET /api/workers/<id>` — one worker plus its recent tasks.
/// * `GET /api/tasks` — recent tasks (filter by `state`/`name`/`queue`/`worker`,
///   paginate with `limit`/`offset`).
/// * `GET /api/tasks/<id>` — one task's full detail.
/// * `POST /api/tasks/fire` — enqueue a new task by name/args/kwargs/queue.
/// * `POST /api/tasks/<id>/retry` — re-fire a historical task (new id).
/// * `POST /api/tasks/<id>/revoke` — broadcast revoke via [control].
/// * `POST /api/queues/<name>/pause` / `.../resume` — pause/resume consumption.
/// * `GET /api/queues` — broker depth per known queue.
/// * `GET /api/events` — Server-Sent Events firehose for live updates.
///
/// Optional [basicAuth] (`"user:pass"`) gates every route with HTTP Basic Auth.
/// Mutating endpoints require [broker] (fire/retry) and/or [control]
/// (revoke/pause/resume); otherwise they return `503`.
///
/// Run it as its own process (see `bin/rose.dart`) pointed at the same Redis
/// the cluster uses, or embed it in-process with an [InMemoryEventBus].
///
/// ```dart
/// final rose = GisilaRose(
///   source: RedisEventSource(host: 'localhost'),
///   broker: RedisBroker(host: 'localhost'),
///   control: RedisControlChannel(host: 'localhost'),
///   events: RedisEventSink(host: 'localhost'),
///   basicAuth: 'admin:secret',
///   port: 5555,
/// );
/// await rose.start();
/// ```
class GisilaRose {
  GisilaRose({
    required this.source,
    this.broker,
    this.control,
    this.events,
    this.backend,
    this.basicAuth,
    MonitorState? state,
    this.host = '0.0.0.0',
    this.port = 5555,
    this.queuePollInterval = const Duration(seconds: 2),
    Uuid? idGenerator,
  })  : state = state ?? MonitorState(),
        _uuid = idGenerator ?? const Uuid();

  /// The live event stream the dashboard folds into [state].
  final QueueEventSource source;

  /// Optional broker used to poll queue depths and to enqueue on fire/retry.
  final Broker? broker;

  /// Optional control channel for revoke / pause / resume.
  final ControlChannel? control;

  /// Optional sink used to emit `taskSent` when Rose fires or retries a task.
  final QueueEventSink? events;

  /// Optional result backend — stores a pending result when Rose enqueues.
  final ResultBackend? backend;

  /// When set (`"user:pass"`), every HTTP route requires Basic Auth.
  final String? basicAuth;

  final MonitorState state;
  final String host;
  final int port;

  /// How often broker queue depths are refreshed.
  final Duration queuePollInterval;

  final Uuid _uuid;

  HttpServer? _server;
  Timer? _queuePoll;
  final Set<HttpResponse> _sseClients = {};

  /// The actual port bound after [start] — useful when [port] was `0`
  /// (bind to any free port), e.g. in tests. Null before [start] / after
  /// [stop].
  int? get boundPort => _server?.port;

  Map<String, Object?> get _capabilities => {
        'control': control != null,
        'fire': broker != null,
        'auth': basicAuth != null,
      };

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
        if (!req.response.headers.chunkedTransferEncoding) {
          _json(req.response, {'error': '$e'}, status: 500);
        }
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
    if (!_authorized(req)) {
      res.statusCode = 401;
      res.headers
          .set(HttpHeaders.wwwAuthenticateHeader, 'Basic realm="GisilaRose"');
      res.headers.contentType = ContentType.json;
      res.write(jsonEncode({'error': 'unauthorized'}));
      await res.close();
      return;
    }

    final path = req.uri.path;
    final method = req.method;

    if ((path == '/' || path == '/index.html') && method == 'GET') {
      res.headers.contentType = ContentType.html;
      res.write(dashboardHtml);
      await res.close();
      return;
    }
    if (path == '/api/stats' && method == 'GET') {
      _json(res, state.stats(DateTime.now(), capabilities: _capabilities));
      return;
    }
    if (path == '/api/workers' && method == 'GET') {
      final now = DateTime.now();
      _json(res, {
        'workers': [
          for (final w in state.workers)
            w.toJson(
              now: now,
              timeout: state.workerTimeout,
              activeTaskIds: state.activeTaskIdsFor(w.id),
            ),
        ],
      });
      return;
    }
    if (path.startsWith('/api/workers/') && method == 'GET') {
      final id = Uri.decodeComponent(path.substring('/api/workers/'.length));
      final w = state.workerById(id);
      if (w == null) {
        _json(res, {'error': 'not found'}, status: 404);
        return;
      }
      final now = DateTime.now();
      final activeIds = state.activeTaskIdsFor(id);
      _json(res, {
        'worker': w.toJson(
          now: now,
          timeout: state.workerTimeout,
          activeTaskIds: activeIds,
        ),
        'active_tasks': [
          for (final tid in activeIds)
            if (state.taskById(tid) != null) state.taskById(tid)!.toJson(),
        ],
        'tasks': [
          for (final t in state.tasksForWorker(id, state: null).take(100))
            t.toJson(),
        ],
      });
      return;
    }
    if (path == '/api/queues' && method == 'GET') {
      _json(res, {'queues': state.queueLengths});
      return;
    }
    if (path.startsWith('/api/queues/') && method == 'POST') {
      await _handleQueueAction(req, res, path);
      return;
    }
    if (path == '/api/tasks' && method == 'GET') {
      final q = req.uri.queryParameters;
      final tasks = state.tasks(
        state: _parseState(q['state']),
        name: q['name'],
        queue: q['queue'],
        worker: q['worker'],
        limit: int.tryParse(q['limit'] ?? '') ?? 200,
        offset: int.tryParse(q['offset'] ?? '') ?? 0,
      );
      _json(res, {
        'total': state.taskCount,
        'tasks': [for (final t in tasks) t.toJson()],
      });
      return;
    }
    if (path == '/api/tasks/fire' && method == 'POST') {
      await _handleFire(req, res);
      return;
    }
    if (path.startsWith('/api/tasks/') && method == 'POST') {
      await _handleTaskAction(req, res, path);
      return;
    }
    if (path.startsWith('/api/tasks/') && method == 'GET') {
      final id = path.substring('/api/tasks/'.length);
      if (id.contains('/')) {
        _json(res, {'error': 'not found'}, status: 404);
        return;
      }
      final t = state.taskById(id);
      if (t == null) {
        _json(res, {'error': 'not found'}, status: 404);
      } else {
        _json(res, t.toJson());
      }
      return;
    }
    if (path == '/api/events' && method == 'GET') {
      await _streamEvents(res);
      return;
    }

    _json(res, {'error': 'not found'}, status: 404);
  }

  Future<void> _handleQueueAction(
    HttpRequest req,
    HttpResponse res,
    String path,
  ) async {
    final rest = path.substring('/api/queues/'.length);
    final slash = rest.lastIndexOf('/');
    if (slash <= 0) {
      _json(res, {'error': 'not found'}, status: 404);
      return;
    }
    final name = Uri.decodeComponent(rest.substring(0, slash));
    final action = rest.substring(slash + 1);
    final c = control;
    if (c == null) {
      _json(res, {'error': 'control channel not configured'}, status: 503);
      return;
    }
    if (action == 'pause') {
      await c.publish(ControlMessage.pause(name));
      _json(res, {'ok': true, 'queue': name, 'action': 'pause'});
      return;
    }
    if (action == 'resume') {
      await c.publish(ControlMessage.resume(name));
      _json(res, {'ok': true, 'queue': name, 'action': 'resume'});
      return;
    }
    _json(res, {'error': 'not found'}, status: 404);
  }

  Future<void> _handleTaskAction(
    HttpRequest req,
    HttpResponse res,
    String path,
  ) async {
    final rest = path.substring('/api/tasks/'.length);
    final slash = rest.lastIndexOf('/');
    if (slash <= 0) {
      _json(res, {'error': 'not found'}, status: 404);
      return;
    }
    final id = Uri.decodeComponent(rest.substring(0, slash));
    final action = rest.substring(slash + 1);

    if (action == 'revoke') {
      final c = control;
      if (c == null) {
        _json(res, {'error': 'control channel not configured'}, status: 503);
        return;
      }
      await c.publish(ControlMessage.revoke(id));
      _json(res, {'ok': true, 'id': id, 'action': 'revoke'});
      return;
    }

    if (action == 'retry') {
      if (broker == null) {
        _json(res, {'error': 'broker not configured'}, status: 503);
        return;
      }
      final t = state.taskById(id);
      if (t == null) {
        _json(res, {'error': 'not found'}, status: 404);
        return;
      }
      try {
        final newId = await _enqueueTask(
          name: t.name,
          args: _asArgs(t.args),
          kwargs: _asKwargs(t.kwargs),
          queue: t.queue ?? 'default',
          maxRetries: t.maxRetries,
        );
        _json(res, {'ok': true, 'id': newId, 'retried_from': id});
      } on Object catch (e) {
        _json(res, {'error': '$e'}, status: 400);
      }
      return;
    }

    _json(res, {'error': 'not found'}, status: 404);
  }

  Future<void> _handleFire(HttpRequest req, HttpResponse res) async {
    if (broker == null) {
      _json(res, {'error': 'broker not configured'}, status: 503);
      return;
    }
    final raw = await utf8.decoder.bind(req).join();
    Map<String, Object?> body;
    try {
      body = (raw.isEmpty ? <String, Object?>{} : jsonDecode(raw) as Map)
          .cast<String, Object?>();
    } on Object {
      _json(res, {'error': 'invalid JSON body'}, status: 400);
      return;
    }
    final name = body['name'] as String?;
    if (name == null || name.isEmpty) {
      _json(res, {'error': 'name is required'}, status: 400);
      return;
    }
    final queue = (body['queue'] as String?)?.trim();
    try {
      final id = await _enqueueTask(
        name: name,
        args: _asArgs(body['args']),
        kwargs: _asKwargs(body['kwargs']),
        queue: (queue == null || queue.isEmpty) ? 'default' : queue,
        maxRetries: (body['max_retries'] as num?)?.toInt() ?? 0,
      );
      _json(res, {'ok': true, 'id': id});
    } on Object catch (e) {
      _json(res, {'error': '$e'}, status: 400);
    }
  }

  Future<String> _enqueueTask({
    required String name,
    required List<Object?> args,
    required Map<String, Object?> kwargs,
    required String queue,
    int maxRetries = 0,
  }) async {
    final b = broker;
    if (b == null) {
      throw StateError('broker not configured');
    }
    validateEncodable(name, args, kwargs);
    final id = _uuid.v4();
    final message = TaskMessage(
      id: id,
      task: name,
      queue: queue,
      args: args,
      kwargs: kwargs,
      maxRetries: maxRetries,
      createdAt: DateTime.now(),
    );
    await backend?.store(
      message.id,
      StoredResult(
        state: TaskState.pending,
        updatedAt: DateTime.now(),
      ),
    );
    await b.enqueue(message);
    final event = QueueEvent(
      type: QueueEventType.taskSent,
      timestamp: DateTime.now(),
      taskId: message.id,
      taskName: message.task,
      queue: message.queue,
      fields: {
        'args': message.args,
        'kwargs': message.kwargs,
        'retries': message.retries,
        'max_retries': message.maxRetries,
      },
    );
    await events?.emit(event);
    // Always fold locally so the dashboard updates even without a sink.
    state.apply(event);
    // Track queue for depth polling.
    state.queueLengths.putIfAbsent(queue, () => 0);
    return id;
  }

  List<Object?> _asArgs(Object? raw) {
    if (raw == null) return const [];
    if (raw is List) return List<Object?>.from(raw);
    throw FormatException('args must be a JSON array');
  }

  Map<String, Object?> _asKwargs(Object? raw) {
    if (raw == null) return const {};
    if (raw is Map) {
      return raw.map((k, v) => MapEntry('$k', v));
    }
    throw FormatException('kwargs must be a JSON object');
  }

  bool _authorized(HttpRequest req) {
    final expected = basicAuth;
    if (expected == null) return true;
    final header = req.headers.value(HttpHeaders.authorizationHeader);
    if (header == null || !header.startsWith('Basic ')) return false;
    try {
      final decoded = utf8.decode(base64Decode(header.substring(6)));
      return decoded == expected;
    } on Object {
      return false;
    }
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
      ..set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
      ..set('Access-Control-Allow-Headers', 'Content-Type, Authorization');
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

  /// Stops serving and releases the event source / broker / control
  /// connections.
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
    await control?.close();
    await events?.close();
    await backend?.close();
  }
}
