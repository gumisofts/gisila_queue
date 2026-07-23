import 'dart:convert';

/// The kind of lifecycle event a producer or worker emits, mirroring the
/// Celery event types that Flower consumes.
enum QueueEventType {
  /// A producer enqueued a task (`GisilaQueue.send`).
  taskSent,

  /// A worker reserved a message off the broker.
  taskReceived,

  /// A worker began executing the handler.
  taskStarted,

  /// The handler returned normally.
  taskSucceeded,

  /// The handler failed terminally (no retries remain).
  taskFailed,

  /// The task was re-enqueued for another attempt.
  taskRetried,

  /// The task was revoked / rejected without requeue.
  taskRevoked,

  /// The task was deferred (rescheduled with a delay, without counting as
  /// an application-level retry) because it exceeded its [Task.rateLimit].
  taskRateLimited,

  /// A worker process started consuming.
  workerOnline,

  /// A periodic worker liveness ping carrying its counters.
  workerHeartbeat,

  /// A worker process stopped consuming cleanly.
  workerOffline;

  /// Whether this event concerns a single task (vs a worker lifecycle).
  bool get isTask => name.startsWith('task');

  /// Whether this event concerns a worker lifecycle.
  bool get isWorker => name.startsWith('worker');

  static QueueEventType parse(String? value) =>
      QueueEventType.values.firstWhere(
        (e) => e.name == value,
        orElse: () => QueueEventType.taskSent,
      );
}

/// A single observability event flowing from producers/workers to any
/// attached [EventSink]. Equivalent to a Celery event body: it identifies the
/// subject (a task id or a worker id), the [QueueEventType], a timestamp, and
/// a small bag of type-specific [fields].
///
/// Events are JSON-encodable so they can travel over Redis pub/sub to a
/// separate monitoring process (see [RedisEventSink]).
class QueueEvent {
  QueueEvent({
    required this.type,
    required this.timestamp,
    this.taskId,
    this.taskName,
    this.queue,
    this.workerId,
    this.hostname,
    Map<String, Object?>? fields,
  }) : fields = fields ?? const {};

  final QueueEventType type;
  final DateTime timestamp;

  /// Task id for task events; null for worker events.
  final String? taskId;

  /// Registered task name for task events.
  final String? taskName;

  /// Queue the task was routed to.
  final String? queue;

  /// Stable id of the worker that produced the event (worker events, plus
  /// task events once a worker has picked the task up).
  final String? workerId;

  /// Human-friendly `host:pid` label of the originating worker.
  final String? hostname;

  /// Type-specific extras — e.g. `runtimeMs`, `result`, `error`, `retries`,
  /// `args`, `kwargs`, `concurrency`, `queues`, `active`, `succeeded`,
  /// `failed`, `retried`.
  final Map<String, Object?> fields;

  /// Convenience getter for a numeric [fields] value.
  num? numField(String key) => fields[key] is num ? fields[key] as num : null;

  Map<String, Object?> toJson() => {
        'type': type.name,
        'ts': timestamp.toUtc().toIso8601String(),
        if (taskId != null) 'task_id': taskId,
        if (taskName != null) 'task_name': taskName,
        if (queue != null) 'queue': queue,
        if (workerId != null) 'worker_id': workerId,
        if (hostname != null) 'hostname': hostname,
        if (fields.isNotEmpty) 'fields': fields,
      };

  factory QueueEvent.fromJson(Map<String, Object?> json) => QueueEvent(
        type: QueueEventType.parse(json['type'] as String?),
        timestamp: json['ts'] is String
            ? DateTime.parse(json['ts']! as String).toLocal()
            : DateTime.fromMillisecondsSinceEpoch(0),
        taskId: json['task_id'] as String?,
        taskName: json['task_name'] as String?,
        queue: json['queue'] as String?,
        workerId: json['worker_id'] as String?,
        hostname: json['hostname'] as String?,
        fields: (json['fields'] as Map?)?.cast<String, Object?>(),
      );

  String encode() => jsonEncode(toJson());

  factory QueueEvent.decode(String raw) =>
      QueueEvent.fromJson(jsonDecode(raw) as Map<String, Object?>);

  @override
  String toString() =>
      'QueueEvent(${type.name} task=$taskName id=$taskId worker=$workerId)';
}
