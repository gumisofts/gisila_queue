/// Base type for every exception thrown by gisila_queue.
abstract class GisilaQueueException implements Exception {
  const GisilaQueueException(this.message);

  final String message;

  @override
  String toString() => '$runtimeType: $message';
}

/// Thrown by a worker when it dequeues a message whose task name is not
/// present in the [GisilaQueue] registry.
class UnknownTaskException extends GisilaQueueException {
  const UnknownTaskException(this.taskName)
      : super('no task registered under "$taskName"');

  final String taskName;
}

/// Raised from inside a task handler (typically via `ctx.retry(...)`) to
/// ask the worker to re-enqueue the task instead of treating the run as a
/// terminal failure.
///
/// The worker honours the request only while `retries < maxRetries`;
/// otherwise it is converted into a [TaskFailureException].
class RetryTask extends GisilaQueueException {
  RetryTask({this.countdown, this.cause})
      : super('task requested retry'
            '${countdown != null ? ' in $countdown' : ''}');

  /// How long to wait before the retry becomes eligible. When null the
  /// task's configured backoff schedule is used.
  final Duration? countdown;

  /// The original error that prompted the retry, surfaced in logs.
  final Object? cause;
}

/// Raised from a handler to drop the message without retrying. When
/// [requeue] is true the worker puts the message back on its queue.
class RejectTask extends GisilaQueueException {
  const RejectTask({this.requeue = false, String? reason})
      : super(reason ?? 'task rejected');

  final bool requeue;
}

/// Raised internally when a task's handler doesn't return within its
/// configured hard [Task.timeLimit].
///
/// Dart offers no way to forcibly preempt a running computation within a
/// single isolate, so this does not stop the handler's work. The run is
/// recorded as timed out (and retried or failed as usual), but its
/// concurrency slot stays busy until the handler actually returns, so
/// runaway handlers cannot pile up beyond the worker's concurrency. Any
/// value or error the abandoned computation produces is discarded. Poll
/// [TaskContext.isAbandoned] or [TaskContext.softTimeLimitExceeded] in long
/// handlers to wind down cooperatively.
class TaskTimeLimitExceededException extends GisilaQueueException {
  const TaskTimeLimitExceededException(this.taskId, this.limit)
      : super('exceeded time limit of $limit');

  final String taskId;
  final Duration limit;
}

/// The terminal failure recorded for a task whose handler raised and whose
/// retries were exhausted. Re-thrown by [AsyncResult.get] when the caller
/// waits on a failed task.
class TaskFailureException extends GisilaQueueException {
  const TaskFailureException(this.taskId, String error, {this.cause})
      : super(error);

  final String taskId;
  final Object? cause;
}

/// Thrown by [AsyncResult.get] when the result does not become available
/// within the requested timeout.
class ResultTimeoutException extends GisilaQueueException {
  const ResultTimeoutException(this.taskId, this.timeout)
      : super('result for "$taskId" not ready within $timeout');

  final String taskId;
  final Duration timeout;
}
