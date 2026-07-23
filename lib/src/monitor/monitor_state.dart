import 'dart:async';
import 'dart:collection';

import '../events/event.dart';
import '../events/event_sink.dart';
import '../task_state.dart';

/// A monitor's view of a single task, folded from its lifecycle events.
class TaskInfo {
  TaskInfo({required this.id, required this.name});

  final String id;
  String name;
  String? queue;
  String? workerId;
  TaskState state = TaskState.pending;

  Object? args;
  Object? kwargs;
  Object? result;
  String? error;

  int retries = 0;
  int maxRetries = 0;
  int? runtimeMs;

  DateTime? sentAt;
  DateTime? receivedAt;
  DateTime? startedAt;
  DateTime? finishedAt;

  /// Last time any event touched this task — used for recency ordering.
  DateTime updatedAt = DateTime.fromMillisecondsSinceEpoch(0);

  Map<String, Object?> toJson() => {
        'id': id,
        'name': name,
        'state': state.name,
        if (queue != null) 'queue': queue,
        if (workerId != null) 'worker_id': workerId,
        if (args != null) 'args': args,
        if (kwargs != null) 'kwargs': kwargs,
        if (result != null) 'result': result,
        if (error != null) 'error': error,
        'retries': retries,
        'max_retries': maxRetries,
        if (runtimeMs != null) 'runtime_ms': runtimeMs,
        if (sentAt != null) 'sent_at': sentAt!.toUtc().toIso8601String(),
        if (receivedAt != null)
          'received_at': receivedAt!.toUtc().toIso8601String(),
        if (startedAt != null)
          'started_at': startedAt!.toUtc().toIso8601String(),
        if (finishedAt != null)
          'finished_at': finishedAt!.toUtc().toIso8601String(),
        'updated_at': updatedAt.toUtc().toIso8601String(),
      };
}

/// A monitor's view of a worker process, folded from its heartbeats.
class WorkerInfo {
  WorkerInfo({required this.id});

  final String id;
  String? hostname;
  List<String> queues = const [];
  int concurrency = 0;
  int active = 0;
  int succeeded = 0;
  int failed = 0;
  int retried = 0;

  /// False once [workerOffline] is seen or heartbeats lapse (see [isAlive]).
  bool online = true;
  DateTime lastHeartbeat = DateTime.fromMillisecondsSinceEpoch(0);

  /// A worker counts as alive if it hasn't signed off and its last heartbeat
  /// is within [timeout] of [now].
  bool isAlive(DateTime now, Duration timeout) =>
      online && now.difference(lastHeartbeat) <= timeout;

  Map<String, Object?> toJson({DateTime? now, Duration? timeout}) => {
        'id': id,
        if (hostname != null) 'hostname': hostname,
        'queues': queues,
        'concurrency': concurrency,
        'active': active,
        'succeeded': succeeded,
        'failed': failed,
        'retried': retried,
        'alive':
            (now != null && timeout != null) ? isAlive(now, timeout) : online,
        'last_heartbeat': lastHeartbeat.toUtc().toIso8601String(),
      };
}

/// Aggregates the [QueueEvent] stream into a live, queryable model of tasks,
/// workers, and counters — the in-memory state a [GisilaRose] dashboard
/// renders. This is the gisila_queue analogue of Flower's event-driven state.
///
/// Task history is bounded to [maxTasks] (newest retained), so a long-running
/// monitor stays memory-stable.
class MonitorState {
  MonitorState({
    this.maxTasks = 10000,
    this.workerTimeout = const Duration(seconds: 30),
  });

  /// Upper bound on retained tasks; oldest are evicted past this.
  final int maxTasks;

  /// How long after its last heartbeat a worker is considered offline.
  final Duration workerTimeout;

  // Insertion-ordered so eviction drops the oldest-touched task.
  final LinkedHashMap<String, TaskInfo> _tasks = LinkedHashMap();
  final Map<String, WorkerInfo> _workers = {};

  /// Lifetime totals keyed by [QueueEventType.name] of terminal/queue events.
  int sent = 0;
  int received = 0;
  int succeeded = 0;
  int failed = 0;
  int retried = 0;
  int revoked = 0;
  int rateLimited = 0;

  /// Latest known broker depth per queue, refreshed by the dashboard poller.
  final Map<String, int> queueLengths = {};

  final StreamController<QueueEvent> _tap =
      StreamController<QueueEvent>.broadcast();

  /// A re-broadcast of every applied event, for the dashboard's SSE tail.
  Stream<QueueEvent> get tap => _tap.stream;

  StreamSubscription<QueueEvent>? _sub;

  /// Folds prior [history] then subscribes to live [QueueEventSource.events].
  Future<void> bind(QueueEventSource source) async {
    for (final e in await source.history(limit: maxTasks)) {
      apply(e, broadcast: false);
    }
    _sub = source.events().listen(apply);
  }

  TaskInfo _task(String id, String name) => _tasks.putIfAbsent(id, () {
        // Re-insert at the end to mark as most-recent, evicting the oldest.
        if (_tasks.length >= maxTasks && _tasks.isNotEmpty) {
          _tasks.remove(_tasks.keys.first);
        }
        return TaskInfo(id: id, name: name);
      });

  WorkerInfo _worker(String id) =>
      _workers.putIfAbsent(id, () => WorkerInfo(id: id));

  /// Folds a single [event] into the model. [broadcast] is false during
  /// history replay so the SSE tap only carries live events.
  void apply(QueueEvent event, {bool broadcast = true}) {
    final ts = event.timestamp;
    if (event.type.isTask && event.taskId != null) {
      final t = _task(event.taskId!, event.taskName ?? '?');
      if (event.taskName != null) t.name = event.taskName!;
      if (event.queue != null) t.queue = event.queue;
      if (event.workerId != null) t.workerId = event.workerId;
      if (ts.isAfter(t.updatedAt)) t.updatedAt = ts;
      final f = event.fields;
      if (f['retries'] is num) t.retries = (f['retries'] as num).toInt();
      if (f['max_retries'] is num) {
        t.maxRetries = (f['max_retries'] as num).toInt();
      }

      switch (event.type) {
        case QueueEventType.taskSent:
          sent++;
          t.state = TaskState.pending;
          t.sentAt = ts;
          t.args = f['args'];
          t.kwargs = f['kwargs'];
          if (f['eta'] is String) t.state = TaskState.scheduled;
          break;
        case QueueEventType.taskReceived:
          received++;
          t.receivedAt = ts;
          t.args ??= f['args'];
          t.kwargs ??= f['kwargs'];
          break;
        case QueueEventType.taskStarted:
          t.state = TaskState.started;
          t.startedAt = ts;
          break;
        case QueueEventType.taskSucceeded:
          succeeded++;
          t.state = TaskState.success;
          t.finishedAt = ts;
          t.result = f['result'];
          if (f['runtime_ms'] is num) {
            t.runtimeMs = (f['runtime_ms'] as num).toInt();
          }
          break;
        case QueueEventType.taskFailed:
          failed++;
          t.state = TaskState.failure;
          t.finishedAt = ts;
          t.error = f['error'] as String?;
          if (f['runtime_ms'] is num) {
            t.runtimeMs = (f['runtime_ms'] as num).toInt();
          }
          break;
        case QueueEventType.taskRetried:
          retried++;
          t.state = TaskState.retry;
          break;
        case QueueEventType.taskRateLimited:
          rateLimited++;
          t.state = TaskState.scheduled;
          break;
        case QueueEventType.taskRevoked:
          revoked++;
          t.state = TaskState.revoked;
          t.finishedAt = ts;
          t.error = f['reason'] as String?;
          break;
        default:
          break;
      }
    } else if (event.type.isWorker && event.workerId != null) {
      final w = _worker(event.workerId!);
      if (event.hostname != null) w.hostname = event.hostname;
      final f = event.fields;
      if (f['queues'] is List) {
        w.queues = (f['queues'] as List).map((e) => '$e').toList();
      }
      if (f['concurrency'] is num) {
        w.concurrency = (f['concurrency'] as num).toInt();
      }
      if (f['active'] is num) w.active = (f['active'] as num).toInt();
      if (f['succeeded'] is num) w.succeeded = (f['succeeded'] as num).toInt();
      if (f['failed'] is num) w.failed = (f['failed'] as num).toInt();
      if (f['retried'] is num) w.retried = (f['retried'] as num).toInt();
      if (ts.isAfter(w.lastHeartbeat)) w.lastHeartbeat = ts;
      w.online = event.type != QueueEventType.workerOffline;
    }

    if (broadcast && _tap.hasListener) _tap.add(event);
  }

  /// Tasks, most-recently-updated first, optionally filtered.
  List<TaskInfo> tasks({
    TaskState? state,
    String? name,
    String? queue,
    int limit = 200,
    int offset = 0,
  }) {
    final all = _tasks.values
        .where((t) => state == null || t.state == state)
        .where((t) => name == null || t.name == name)
        .where((t) => queue == null || t.queue == queue)
        .toList()
      ..sort((a, b) => b.updatedAt.compareTo(a.updatedAt));
    if (offset >= all.length) return const [];
    return all.sublist(offset, (offset + limit).clamp(0, all.length));
  }

  TaskInfo? taskById(String id) => _tasks[id];

  int get taskCount => _tasks.length;

  List<WorkerInfo> get workers => _workers.values.toList();

  /// Per-task-name aggregate counts over retained tasks.
  Map<String, Map<String, int>> taskTypeStats() {
    final out = <String, Map<String, int>>{};
    for (final t in _tasks.values) {
      final m = out.putIfAbsent(t.name, () => {'total': 0});
      m['total'] = (m['total'] ?? 0) + 1;
      m[t.state.name] = (m[t.state.name] ?? 0) + 1;
    }
    return out;
  }

  /// Dashboard summary payload.
  Map<String, Object?> stats(DateTime now) {
    final aliveWorkers =
        _workers.values.where((w) => w.isAlive(now, workerTimeout)).length;
    return {
      'totals': {
        'sent': sent,
        'received': received,
        'succeeded': succeeded,
        'failed': failed,
        'retried': retried,
        'revoked': revoked,
        'rate_limited': rateLimited,
      },
      'workers': {
        'total': _workers.length,
        'alive': aliveWorkers,
      },
      'tasks_tracked': _tasks.length,
      'queues': Map.of(queueLengths),
      'task_types': taskTypeStats(),
    };
  }

  Future<void> close() async {
    await _sub?.cancel();
    await _tap.close();
  }
}
