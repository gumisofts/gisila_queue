/// Lifecycle states a task moves through, mirroring Celery's task states.
enum TaskState {
  /// Accepted by the broker, not yet picked up by a worker.
  pending,

  /// Held back for a future [DateTime] via an ETA / countdown.
  scheduled,

  /// A worker has begun executing the task.
  started,

  /// The handler returned normally; a result may be available.
  success,

  /// The handler raised and no retries remain.
  failure,

  /// The handler asked to retry (or raised with retries remaining); the
  /// task has been re-enqueued.
  retry,

  /// The task was revoked before (or during) execution.
  revoked;

  /// Whether this is a terminal state (no further transitions expected).
  bool get isDone =>
      this == TaskState.success ||
      this == TaskState.failure ||
      this == TaskState.revoked;

  /// Parses a [TaskState] from its [name], defaulting to [pending].
  static TaskState parse(String? value) {
    return TaskState.values.firstWhere(
      (s) => s.name == value,
      orElse: () => TaskState.pending,
    );
  }
}
