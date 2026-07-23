import 'app.dart';
import 'backend/result_backend.dart';
import 'control.dart';
import 'exceptions.dart';
import 'task_state.dart';

/// A producer-side handle to a single enqueued task, mirroring Celery's
/// `AsyncResult`. Lets you poll state and await the return value through the
/// configured [ResultBackend].
class AsyncResult<T> {
  AsyncResult(this._app, this.id);

  final GisilaQueue _app;

  /// Task id; the result-backend key.
  final String id;

  ResultBackend get _backend {
    final b = _app.backend;
    if (b == null) {
      throw StateError(
          'GisilaQueue has no result backend; cannot read results. '
          'Pass a backend to GisilaQueue() to use AsyncResult.');
    }
    return b;
  }

  Future<StoredResult?> _snapshot() => _backend.fetch(id);

  /// Current [TaskState], or [TaskState.pending] if nothing is stored yet.
  Future<TaskState> get state async =>
      (await _snapshot())?.state ?? TaskState.pending;

  /// Whether the task has reached a terminal state.
  Future<bool> get ready async => (await _snapshot())?.state.isDone ?? false;

  /// Whether the task completed successfully.
  Future<bool> get successful async =>
      (await _snapshot())?.state == TaskState.success;

  /// Waits for the task to finish and returns its value.
  ///
  /// Polls the backend every [pollInterval] until a terminal state is
  /// reached or [timeout] elapses. Throws [TaskFailureException] if the task
  /// failed and [ResultTimeoutException] if it never completes in time.
  Future<T?> get({
    Duration timeout = const Duration(seconds: 30),
    Duration pollInterval = const Duration(milliseconds: 100),
  }) async {
    final deadline = DateTime.now().add(timeout);
    while (true) {
      final snap = await _snapshot();
      if (snap != null && snap.state.isDone) {
        if (snap.state == TaskState.failure) {
          throw TaskFailureException(id, snap.error ?? 'task failed');
        }
        if (snap.state == TaskState.revoked) {
          throw TaskFailureException(id, 'task was revoked');
        }
        return snap.result as T?;
      }
      if (!DateTime.now().isBefore(deadline)) {
        throw ResultTimeoutException(id, timeout);
      }
      await Future<void>.delayed(pollInterval);
    }
  }

  /// Drops the stored result for this task.
  Future<void> forget() => _backend.forget(id);

  /// Asks every worker not to run this task — Celery's `AsyncResult.revoke`.
  ///
  /// Only prevents the task from *starting*; a task already running when
  /// the revoke is published keeps running to completion (see
  /// [ControlMessage.terminate] for why `terminate: true` isn't currently
  /// enforced). If a [ResultBackend] is configured, the state is also
  /// marked [TaskState.revoked] immediately, so [get]/[state] reflect it
  /// right away even if no worker is currently subscribed to pick up the
  /// broadcast.
  Future<void> revoke({bool terminate = false}) async {
    await _app.backend?.store(
      id,
      StoredResult(
        state: TaskState.revoked,
        error: 'revoked by producer',
        updatedAt: DateTime.now(),
      ),
    );
    await _app.controlChannel
        .publish(ControlMessage.revoke(id, terminate: terminate));
  }
}
