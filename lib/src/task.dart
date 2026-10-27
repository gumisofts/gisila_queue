import 'dart:math' as math;

import 'app.dart';
import 'canvas.dart';
import 'exceptions.dart';
import 'logging.dart';
import 'rate_limit.dart';
import 'result.dart';

/// The signature every task handler implements. Receives a [TaskContext]
/// and returns a JSON-encodable value (or null) that becomes the task's
/// result.
typedef TaskHandler = Future<Object?> Function(TaskContext ctx);

/// Per-invocation context handed to a [TaskHandler] when a worker executes
/// a task. Exposes the call arguments plus retry controls.
class TaskContext {
  TaskContext({
    required this.id,
    required this.task,
    required this.args,
    required this.kwargs,
    required this.retries,
    required this.maxRetries,
    required this.queue,
    required this.logger,
    DateTime? startedAt,
    this.softTimeLimit,
    bool Function()? isRevokedCheck,
  })  : startedAt = startedAt ?? DateTime.now(),
        _isRevokedCheck = isRevokedCheck;

  bool _abandoned = false;

  /// Whether the worker stopped waiting for this run because it passed the
  /// task's hard [Task.timeLimit]. The run has already been recorded as
  /// failed (or retried); anything it returns from now on is discarded.
  /// Long-running handlers should poll this and return early, since the
  /// worker keeps this run's concurrency slot busy until the handler ends.
  bool get isAbandoned => _abandoned;

  /// Called by the worker when [Task.timeLimit] passes.
  void markAbandoned() => _abandoned = true;

  /// Unique id of this invocation (matches the [AsyncResult] id).
  final String id;

  /// Registered name of the task being run.
  final String task;

  /// Positional arguments the task was called with.
  final List<Object?> args;

  /// Keyword arguments the task was called with.
  final Map<String, Object?> kwargs;

  /// How many times this task has already been retried (0 on first run).
  final int retries;

  /// Configured retry ceiling.
  final int maxRetries;

  /// Queue this invocation was reserved from.
  final String queue;

  /// Worker logger, scoped to this run's context.
  final QueueLogger logger;

  /// When the worker began executing this run.
  final DateTime startedAt;

  /// This task's configured soft time limit, if any. See
  /// [softTimeLimitExceeded].
  final Duration? softTimeLimit;

  final bool Function()? _isRevokedCheck;

  /// Whether a producer has called [AsyncResult.revoke] for this run since
  /// it started. Purely cooperative — nothing stops the handler
  /// automatically; long-running or loop-based handlers should poll this
  /// and wind down early (e.g. throw [RejectTask]) instead of continuing.
  bool get isRevoked => _isRevokedCheck?.call() ?? false;

  /// Convenience accessor for positional argument [i].
  Object? arg(int i) => i < args.length ? args[i] : null;

  /// Whether at least one retry attempt remains.
  bool get canRetry => retries < maxRetries;

  /// Whether [softTimeLimit] has elapsed since [startedAt].
  ///
  /// Purely cooperative — nothing enforces this automatically. Long-running
  /// or loop-based handlers should poll it and wind down early (e.g. save
  /// partial progress, then [retry] or return) instead of relying on the
  /// hard [Task.timeLimit], which only abandons the wait rather than
  /// stopping the handler's work.
  bool get softTimeLimitExceeded =>
      softTimeLimit != null &&
      DateTime.now().difference(startedAt) >= softTimeLimit!;

  /// Aborts the current run and asks the worker to re-enqueue this task.
  ///
  /// Honoured only while [canRetry]; otherwise the worker records a terminal
  /// failure. Pass [countdown] to override the task's backoff for this
  /// attempt. Always throws — it never returns.
  Never retry({Duration? countdown, Object? cause}) =>
      throw RetryTask(countdown: countdown, cause: cause);
}

/// Immutable definition of a task: its name, handler, and execution policy.
/// Created via [GisilaQueue.task]; you usually interact with the returned
/// [TaskRef] rather than this directly.
class Task {
  Task({
    required this.name,
    required this.handler,
    this.queue,
    this.maxRetries = 0,
    this.retryBackoff = const Duration(seconds: 1),
    this.retryBackoffMax = const Duration(minutes: 10),
    this.retryJitter = true,
    this.autoRetry = false,
    this.timeLimit,
    this.softTimeLimit,
    this.rateLimit,
  });

  final String name;
  final TaskHandler handler;

  /// Default queue for this task; falls back to the app default when null.
  final String? queue;

  /// Maximum retry attempts before the task is marked failed.
  final int maxRetries;

  /// Base delay used to compute exponential backoff between retries.
  final Duration retryBackoff;

  /// Cap applied to the exponential backoff.
  final Duration retryBackoffMax;

  /// Whether to add random jitter to backoff to avoid thundering herds.
  final bool retryJitter;

  /// When true, any uncaught exception triggers a retry (while attempts
  /// remain) instead of an immediate failure.
  final bool autoRetry;

  /// Hard ceiling on a single run's duration. When exceeded, the worker
  /// stops waiting on the handler and treats the run as a failure (subject
  /// to [autoRetry] / [maxRetries] like any other exception) — see
  /// [TaskTimeLimitExceededException] for the preemption caveat. Null (the
  /// default) means no limit.
  final Duration? timeLimit;

  /// A softer, cooperative ceiling — see [TaskContext.softTimeLimitExceeded].
  /// Not enforced by the worker; purely informational for the handler.
  final Duration? softTimeLimit;

  /// Caps how often this task may run within a single worker process, e.g.
  /// `RateLimit.parse('10/s')`. Null (the default) means unlimited.
  final RateLimit? rateLimit;

  /// Exponential backoff for the given [retryNumber] (1-based), capped at
  /// [retryBackoffMax] and optionally jittered. [rng] is injectable for
  /// deterministic tests.
  Duration backoffFor(int retryNumber, {math.Random? rng}) {
    final factor = math.pow(2, math.max(0, retryNumber - 1)).toInt();
    var ms = retryBackoff.inMilliseconds * factor;
    ms = math.min(ms, retryBackoffMax.inMilliseconds);
    if (retryJitter && ms > 0) {
      final r = rng ?? math.Random();
      // Full jitter: pick uniformly in [ms/2, ms].
      ms = (ms ~/ 2) + r.nextInt((ms ~/ 2) + 1);
    }
    return Duration(milliseconds: ms);
  }
}

/// Producer-side handle to a registered [Task]. Use it to enqueue
/// invocations from anywhere holding the [GisilaQueue].
///
/// ```dart
/// final sendEmail = app.task('emails.send', handler, maxRetries: 3);
/// final r = await sendEmail.delay(['user@example.com']);
/// ```
class TaskRef<T> {
  TaskRef(this._app, this.task);

  final GisilaQueue _app;
  final Task task;

  String get name => task.name;

  /// Builds a [Signature] for this task — an unevaluated call, ready to be
  /// composed into a [GisilaQueue.chain] or [GisilaQueue.group]. Retry
  /// policy defaults to this task's own [Task.maxRetries] unless
  /// overridden.
  Signature sig({
    List<Object?> args = const [],
    Map<String, Object?> kwargs = const {},
    String? queue,
    int? maxRetries,
  }) =>
      Signature(
        task.name,
        args: args,
        kwargs: kwargs,
        queue: queue ?? task.queue,
        maxRetries: maxRetries ?? task.maxRetries,
      );

  /// Shorthand for [applyAsync] with just positional [args].
  Future<AsyncResult<T>> delay([
    List<Object?> args = const [],
    Map<String, Object?> kwargs = const {},
  ]) =>
      applyAsync(args: args, kwargs: kwargs);

  /// Enqueues an invocation, returning an [AsyncResult] to await its
  /// outcome.
  ///
  /// * [queue] overrides everything else (the app's [Router], the task's
  ///   own registered default queue, and [GisilaQueue.defaultQueue]) — see
  ///   [GisilaQueue.send] for the full precedence order when it's omitted.
  /// * [countdown] delays execution by a relative duration.
  /// * [eta] pins an absolute earliest execution time ([countdown] wins if
  ///   both are given).
  /// * [expiresIn] / [expires] set the latest time this call may still
  ///   usefully run; past that, a worker revokes it instead of executing it
  ///   ([expiresIn] wins if both are given).
  Future<AsyncResult<T>> applyAsync({
    List<Object?> args = const [],
    Map<String, Object?> kwargs = const {},
    String? queue,
    Duration? countdown,
    DateTime? eta,
    Duration? expiresIn,
    DateTime? expires,
  }) {
    final effectiveEta =
        countdown != null ? DateTime.now().add(countdown) : eta;
    final effectiveExpires =
        expiresIn != null ? DateTime.now().add(expiresIn) : expires;
    return _app.send<T>(
      task.name,
      args: args,
      kwargs: kwargs,
      queue: queue,
      eta: effectiveEta,
      maxRetries: task.maxRetries,
      expires: effectiveExpires,
    );
  }
}
