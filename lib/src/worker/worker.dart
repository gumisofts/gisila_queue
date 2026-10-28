import 'dart:async';
import 'dart:io';

import '../app.dart';
import '../backend/result_backend.dart';
import '../canvas.dart';
import '../control.dart';
import '../events/event.dart';
import '../exceptions.dart';
import '../message.dart';
import '../namespace.dart';
import '../rate_limit.dart';
import '../task.dart';
import '../task_state.dart';

/// Observability hook fired for each terminal task outcome.
typedef TaskObserver = void Function(TaskMessage message, StoredResult result);

/// Consumes messages from the app's broker and executes the matching tasks,
/// handling retries, backoff, and result storage. Analogous to the Celery
/// `worker` process — except concurrency here is cooperative async within a
/// single isolate (great for IO-bound tasks); scale CPU-bound work by
/// launching additional worker processes.
class Worker {
  Worker(
    this.app, {
    required this.queues,
    this.concurrency = 1,
    this.reserveTimeout = const Duration(seconds: 5),
    String? id,
    this.heartbeatInterval = const Duration(seconds: 10),
    this.deadLetterQueue = defaultDeadLetterQueue,
    this.maxRequeues = 10,
    this.maxRedeliveries = 5,
    this.unknownTaskBackoff = const Duration(seconds: 1),
  }) : id = id ?? '${_hostname()}:$pid';

  /// Default [deadLetterQueue].
  static const defaultDeadLetterQueue = 'dead_letter';

  final GisilaQueue app;

  /// Queues consumed in priority order — earlier queues are drained first.
  final List<String> queues;

  /// Number of messages processed concurrently within this process.
  final int concurrency;

  /// How long each reserve call blocks waiting for work before looping.
  final Duration reserveTimeout;

  /// Stable identity reported on every worker [QueueEvent], so a dashboard can
  /// group a process's activity and detect when it goes offline.
  final String id;

  /// How often a [QueueEventType.workerHeartbeat] is emitted while running.
  final Duration heartbeatInterval;

  /// Where messages that failed for good are put: failures after their
  /// last retry, task names no worker knew after [maxRequeues] tries, and
  /// messages past [maxRequeues] or
  /// [maxRedeliveries]. Each carries [TaskMessage.deadLetter] with the
  /// reason; [GisilaQueue.replayDeadLetters] sends them back. Don't list
  /// this queue in [queues]. Null turns dead-lettering off (failures then
  /// only live in the result backend).
  final String? deadLetterQueue;

  /// How often a message may be put back with `RejectTask(requeue: true)`,
  /// or for an unknown task name, before it is dead-lettered instead.
  final int maxRequeues;

  /// First delay before a message whose task name this worker doesn't know
  /// is offered again; it doubles on each requeue, up to a minute. The
  /// message may belong to a worker that registers the task (a newer
  /// deploy, or another app misconfigured onto this namespace), so it is
  /// put back instead of failed, and dead-lettered only after [maxRequeues].
  final Duration unknownTaskBackoff;

  /// How often a broker may redeliver a message whose worker never acked it
  /// (it crashed, or the process was killed) before it is dead-lettered
  /// instead of run again: a message that keeps taking workers down with
  /// it should stop doing so.
  final int maxRedeliveries;

  /// Invoked after each successful task.
  TaskObserver? onSuccess;

  /// Invoked after each terminal failure.
  TaskObserver? onFailure;

  /// Invoked whenever a task is re-enqueued for retry.
  TaskObserver? onRetry;

  bool _running = false;
  final List<Future<void>> _loops = [];
  Timer? _heartbeat;

  // Lifetime counters surfaced through worker heartbeats.
  int _active = 0;
  int _succeeded = 0;
  int _failed = 0;
  int _retried = 0;
  int _rateLimited = 0;
  int _expired = 0;
  int _revoked = 0;
  int _deadLettered = 0;
  int _abandoned = 0;

  // One sliding-window limiter per rate-limited task name, shared by every
  // concurrency slot in this worker process (matches Celery's per-worker,
  // not cluster-wide, rate-limit semantics).
  final Map<String, SlidingWindowLimiter> _rateLimiters = {};

  // Populated from app.controlChannel — see ControlMessage. Revoked ids are
  // timestamped so a periodic sweep can prune ones old enough that the
  // matching message could never still be sitting on a broker.
  final Map<String, DateTime> _revokedIds = {};
  final Set<String> _pausedQueues = {};
  StreamSubscription<ControlMessage>? _controlSub;

  /// How long a revoked id is remembered before being pruned — should
  /// comfortably exceed how long a message could realistically still be
  /// queued (ETA + retries) after it was revoked.
  static const _revokedIdRetention = Duration(hours: 1);

  static String _hostname() {
    try {
      return Platform.localHostname;
    } catch (_) {
      return 'worker';
    }
  }

  /// Whether the worker is currently consuming.
  bool get isRunning => _running;

  /// Starts the consumer loops and resolves only after [stop] is called
  /// (or an unrecoverable error). Run this as the body of a worker process.
  Future<void> run() async {
    if (_running) return;
    _running = true;
    app.logger.info(
        'worker started: queues=${queues.join(',')} concurrency=$concurrency');
    if (app.namespace == defaultNamespace) {
      app.logger.warning(defaultNamespaceWarning('worker $id'));
    }
    _controlSub = app.controlChannel.subscribe().listen(
          _handleControlMessage,
          onError: (Object e, StackTrace st) =>
              app.logger.warning('control channel error: $e'),
        );
    await _emitWorker(QueueEventType.workerOnline);
    _heartbeat = Timer.periodic(heartbeatInterval, (_) {
      _emitWorker(QueueEventType.workerHeartbeat);
      _pruneRevokedIds();
    });
    for (var i = 0; i < concurrency; i++) {
      _loops.add(_consumeLoop(i));
    }
    await Future.wait(_loops);
    app.logger.info('worker stopped');
  }

  void _handleControlMessage(ControlMessage message) {
    switch (message.type) {
      case ControlMessageType.revoke:
        final taskId = message.taskId;
        if (taskId != null) _revokedIds[taskId] = DateTime.now();
        break;
      case ControlMessageType.pause:
        final queue = message.queue;
        if (queue != null) {
          _pausedQueues.add(queue);
          app.logger.info('worker $id paused queue "$queue"');
        }
        break;
      case ControlMessageType.resume:
        final queue = message.queue;
        if (queue != null) {
          _pausedQueues.remove(queue);
          app.logger.info('worker $id resumed queue "$queue"');
        }
        break;
    }
  }

  /// Checks whether [taskId] has been revoked — first via the local set
  /// populated from live [ControlMessage.revoke] broadcasts (no network
  /// round trip), then, if unset and a backend is configured, via a direct
  /// backend read. The fallback covers the gap a pure broadcast can't:
  /// a revoke published before this worker subscribed (e.g. it hadn't
  /// started yet), which [AsyncResult.revoke] already persists to the
  /// backend for exactly this reason.
  Future<bool> _isRevoked(String taskId) async {
    if (_revokedIds.containsKey(taskId)) return true;
    final backend = app.backend;
    if (backend == null) return false;
    final snap = await backend.fetch(taskId);
    return snap?.state == TaskState.revoked;
  }

  void _pruneRevokedIds() {
    final cutoff = DateTime.now().subtract(_revokedIdRetention);
    _revokedIds.removeWhere((_, seenAt) => seenAt.isBefore(cutoff));
  }

  /// Emits a worker lifecycle event carrying the current counters/config.
  Future<void> _emitWorker(QueueEventType type) async {
    await app.events.emit(QueueEvent(
      type: type,
      timestamp: DateTime.now(),
      workerId: id,
      hostname: id,
      fields: {
        'queues': queues,
        'concurrency': concurrency,
        'active': _active,
        'succeeded': _succeeded,
        'failed': _failed,
        'retried': _retried,
        'rate_limited': _rateLimited,
        'expired': _expired,
        'revoked': _revoked,
        'dead_lettered': _deadLettered,
        'abandoned': _abandoned,
        'paused_queues': _pausedQueues.toList(),
      },
    ));
  }

  /// Emits a task lifecycle event tagged with this worker.
  Future<void> _emitTask(
    QueueEventType type,
    TaskMessage message, {
    Map<String, Object?> fields = const {},
  }) async {
    await app.events.emit(QueueEvent(
      type: type,
      timestamp: DateTime.now(),
      taskId: message.id,
      taskName: message.task,
      queue: message.queue,
      workerId: id,
      hostname: id,
      fields: {
        'retries': message.retries,
        'max_retries': message.maxRetries,
        ...fields,
      },
    ));
  }

  /// Signals every consumer loop to stop after its current task and waits
  /// for them to drain.
  ///
  /// With [timeout], gives up waiting after that long: tasks still running
  /// are left unacked, so a broker with a visibility timeout hands them to
  /// another worker.
  Future<void> stop({Duration? timeout}) async {
    if (!_running) return;
    _running = false;
    _heartbeat?.cancel();
    _heartbeat = null;
    final drained = Future.wait(_loops);
    if (timeout == null) {
      await drained;
    } else {
      await drained.timeout(timeout, onTimeout: () {
        app.logger.warning('worker $id: $_active task(s) still running '
            'after ${timeout.inSeconds}s; stopping without them');
        return const [];
      });
    }
    _loops.clear();
    await _controlSub?.cancel();
    _controlSub = null;
    await _emitWorker(QueueEventType.workerOffline);
  }

  /// Max single blocking-reserve slice, so [stop] is honoured promptly even
  /// when [reserveTimeout] is long.
  static const _drainSlice = Duration(milliseconds: 500);

  /// Reserves a message, splitting [reserveTimeout] into slices so the loop
  /// re-checks [_running] (and thus shuts down) without waiting the full
  /// timeout on an idle queue.
  Future<TaskMessage?> _reserve() async {
    final deadline = DateTime.now().add(reserveTimeout);
    while (_running) {
      final remaining = deadline.difference(DateTime.now());
      if (remaining <= Duration.zero) return null;
      final slice = remaining < _drainSlice ? remaining : _drainSlice;
      final active = _pausedQueues.isEmpty
          ? queues
          : queues.where((q) => !_pausedQueues.contains(q)).toList();
      if (active.isEmpty) {
        // Every queue this worker consumes is paused — there's nothing to
        // ask the broker for; just wait out the slice and re-check.
        await Future<void>.delayed(slice);
        continue;
      }
      final message = await app.broker.reserve(active, timeout: slice);
      if (message != null) return message;
    }
    return null;
  }

  Future<void> _consumeLoop(int slot) async {
    while (_running) {
      TaskMessage? message;
      try {
        message = await _reserve();
      } catch (e, st) {
        app.logger.error('reserve failed on slot $slot', error: e, stack: st);
        await Future<void>.delayed(const Duration(seconds: 1));
        continue;
      }
      if (message == null) continue;
      final lease = _keepReserved(message);
      try {
        await _process(message);
      } catch (e, st) {
        // A failure outside the handler (result backend, event sink, broker
        // ack...) must not end this slot. The message stays unacked, so a
        // reliable broker redelivers it after its visibility timeout.
        app.logger.error(
            'processing ${message.task} id=${message.id} failed '
            'on slot $slot',
            error: e,
            stack: st);
        await Future<void>.delayed(const Duration(seconds: 1));
      } finally {
        lease?.cancel();
      }
    }
  }

  /// While [message] is being processed, periodically [Broker.touch]es it
  /// so a broker with a visibility timeout doesn't redeliver a task that is
  /// merely slow. Returns null when the broker doesn't need it.
  Timer? _keepReserved(TaskMessage message) {
    final timeout = app.broker.visibilityTimeout;
    if (timeout == null) return null;
    var interval = timeout ~/ 3;
    if (interval < const Duration(milliseconds: 50)) {
      interval = const Duration(milliseconds: 50);
    }
    return Timer.periodic(interval, (_) {
      app.broker.touch(message).then<void>((_) {}, onError: (Object e) {
        app.logger.warning('touch failed id=${message.id}: $e');
      });
    });
  }

  Future<void> _process(TaskMessage message) async {
    await _emitTask(QueueEventType.taskReceived, message, fields: {
      'args': message.args,
      'kwargs': message.kwargs,
    });
    final task = app.lookup(message.task);
    if (task == null && message.requeues < maxRequeues) {
      final delay = _unknownTaskDelay(message.requeues);
      app.logger.warning('unknown task "${message.task}" id=${message.id}: '
          'put back for ${delay.inMilliseconds}ms '
          '(${message.requeues + 1}/$maxRequeues); is it registered, and is '
          'this worker on the right namespace?');
      await app.broker.enqueue(message.copyWith(
          requeues: message.requeues + 1, eta: DateTime.now().add(delay)));
      await app.broker.ack(message);
      await _emitTask(QueueEventType.taskRetried, message,
          fields: {'requeued': true, 'unknown_task': true});
      return;
    }
    if (task == null) {
      final err = 'unknown task "${message.task}": no worker ran it after '
          '${message.requeues} requeues';
      app.logger.error('$err id=${message.id}');
      await _record(
        message,
        StoredResult(
          state: TaskState.failure,
          error: err,
          updatedAt: DateTime.now(),
        ),
      );
      _failed++;
      await _emitTask(QueueEventType.taskFailed, message,
          fields: {'error': err});
      await _deadLetter(message, err);
      await app.broker.ack(message);
      return;
    }

    if (message.redeliveries > maxRedeliveries) {
      final err = 'redelivered ${message.redeliveries} times without being '
          'acked (limit $maxRedeliveries)';
      app.logger.error('task ${message.task} ✗ $err id=${message.id}');
      await _record(
        message,
        StoredResult(
            state: TaskState.failure, error: err, updatedAt: DateTime.now()),
      );
      _failed++;
      await _emitTask(QueueEventType.taskFailed, message,
          fields: {'error': err});
      await _deadLetter(message, err);
      await app.broker.ack(message);
      return;
    }

    if (await _isRevoked(message.id)) {
      final err = 'revoked before it started';
      app.logger.warning('task ${message.task} ⛔ $err id=${message.id}');
      await _record(
        message,
        StoredResult(
            state: TaskState.revoked, error: err, updatedAt: DateTime.now()),
      );
      _revoked++;
      await _emitTask(QueueEventType.taskRevoked, message,
          fields: {'reason': err});
      await app.broker.ack(message);
      return;
    }

    final now = DateTime.now();
    if (message.isExpired(now)) {
      final err = 'expired at ${message.expires!.toIso8601String()}';
      app.logger.warning('task ${message.task} ⌛ $err id=${message.id}');
      await _record(
        message,
        StoredResult(state: TaskState.revoked, error: err, updatedAt: now),
      );
      _expired++;
      await _emitTask(QueueEventType.taskRevoked, message,
          fields: {'reason': err});
      await app.broker.ack(message);
      return;
    }

    if (task.rateLimit != null) {
      final limiter = _rateLimiters.putIfAbsent(
          task.name, () => SlidingWindowLimiter(task.rateLimit!));
      final wait = limiter.tryAcquire(now);
      if (wait != null) {
        await _deferForRateLimit(message, wait);
        return;
      }
    }

    final ctx = TaskContext(
      id: message.id,
      task: message.task,
      args: message.args,
      kwargs: message.kwargs,
      retries: message.retries,
      maxRetries: message.maxRetries,
      queue: message.queue,
      logger: app.logger,
      softTimeLimit: task.softTimeLimit,
      isRevokedCheck: () => _revokedIds.containsKey(message.id),
    );

    await app.backend?.store(
      message.id,
      StoredResult(state: TaskState.started, updatedAt: DateTime.now()),
    );
    _active++;
    await _emitTask(QueueEventType.taskStarted, message);

    final sw = Stopwatch()..start();
    Future<void>? stillRunning;
    try {
      final value = await _runHandler(task, ctx, (f) => stillRunning = f);
      final result = StoredResult(
        state: TaskState.success,
        result: value,
        updatedAt: DateTime.now(),
      );
      await _record(message, result);
      _succeeded++;
      await _emitTask(QueueEventType.taskSucceeded, message, fields: {
        'runtime_ms': sw.elapsedMilliseconds,
        'result': value,
      });
      onSuccess?.call(message, result);
      app.logger.info(
          'task ${message.task} ✓ id=${message.id} ${sw.elapsedMilliseconds}ms');
    } on RetryTask catch (r) {
      await _handleRetry(task, message, countdown: r.countdown, cause: r.cause);
    } on RejectTask catch (r) {
      app.logger.warning('task ${message.task} rejected id=${message.id}'
          '${r.requeue ? ' (requeued)' : ''}');
      if (r.requeue && message.requeues >= maxRequeues) {
        final err = 'requeued ${message.requeues} times (limit $maxRequeues)'
            '${r.message.isEmpty ? '' : ': ${r.message}'}';
        final result = StoredResult(
          state: TaskState.failure,
          error: err,
          updatedAt: DateTime.now(),
        );
        await _record(message, result);
        _failed++;
        await _emitTask(QueueEventType.taskFailed, message,
            fields: {'error': err});
        onFailure?.call(message, result);
        await _deadLetter(message, err);
      } else if (r.requeue) {
        await app.broker
            .enqueue(message.copyWith(requeues: message.requeues + 1));
        await _emitTask(QueueEventType.taskRetried, message,
            fields: {'requeued': true});
      } else {
        await _record(
          message,
          StoredResult(
            state: TaskState.revoked,
            error: r.message,
            updatedAt: DateTime.now(),
          ),
        );
        await _emitTask(QueueEventType.taskRevoked, message,
            fields: {'reason': r.message});
      }
    } catch (e, st) {
      if (task.autoRetry && message.retries < message.maxRetries) {
        await _handleRetry(task, message, cause: e);
      } else {
        final result = StoredResult(
          state: TaskState.failure,
          error: e.toString(),
          updatedAt: DateTime.now(),
        );
        await _record(message, result);
        _failed++;
        await _emitTask(QueueEventType.taskFailed, message, fields: {
          'error': e.toString(),
          'runtime_ms': sw.elapsedMilliseconds,
        });
        onFailure?.call(message, result);
        app.logger.error('task ${message.task} ✗ id=${message.id}',
            error: e, stack: st);
        await _deadLetter(message, e.toString());
      }
    } finally {
      _active--;
      await app.broker.ack(message);
    }
    final running = stillRunning;
    if (running != null) {
      await _holdSlotWhile(message, running, task.timeLimit!);
    }
  }

  /// Keeps the calling slot busy until an abandoned handler ([running])
  /// really ends, so handlers past their time limit count against
  /// [concurrency] instead of piling up. Warns every [limit] while it lasts,
  /// and gives up waiting when the worker stops.
  Future<void> _holdSlotWhile(
      TaskMessage message, Future<void> running, Duration limit) async {
    _abandoned++;
    try {
      var waited = limit;
      while (true) {
        final ended = await running
            .then((_) => true)
            .timeout(limit, onTimeout: () => false);
        if (ended) return;
        waited += limit;
        app.logger.warning('task ${message.task} id=${message.id} still '
            'running $waited after it started (time limit $limit); its '
            'slot stays busy until it returns');
        if (!_running) return;
      }
    } finally {
      _abandoned--;
    }
  }

  /// Puts [message] on [deadLetterQueue] with [reason]. Failing to do so is
  /// logged, not thrown: the failure is already recorded.
  Duration _unknownTaskDelay(int requeues) {
    const cap = Duration(minutes: 1);
    final ms = unknownTaskBackoff.inMilliseconds << requeues.clamp(0, 20);
    return ms >= cap.inMilliseconds ? cap : Duration(milliseconds: ms);
  }

  Future<void> _deadLetter(TaskMessage message, String reason) async {
    final queue = deadLetterQueue;
    if (queue == null) return;
    try {
      await app.broker.enqueue(message.copyWith(
        queue: queue,
        clearEta: true,
        deadLetter: {
          'reason': reason,
          'queue': message.deadLetter?['queue'] ?? message.queue,
          'at': DateTime.now().toUtc().toIso8601String(),
        },
      ));
      _deadLettered++;
      app.logger.warning(
          'task ${message.task} id=${message.id} dead-lettered to "$queue"');
    } catch (e, st) {
      app.logger.error(
          'dead-lettering ${message.task} id=${message.id} '
          'failed',
          error: e,
          stack: st);
    }
  }

  /// Runs [task]'s handler, honouring [Task.timeLimit] if set.
  ///
  /// Dart cannot forcibly preempt a running computation, so a time limit
  /// only abandons the *wait*: the run is reported as timed out, and
  /// [onAbandoned] receives a future that completes when the handler really
  /// ends (see [_holdSlotWhile]). Any result or error it produces after
  /// that point is discarded (via an attached no-op listener) so it can't
  /// surface as an unhandled async error.
  ///
  /// Important: do not use [Future.catchError] with `=> null` here. Handlers
  /// often return non-nullable types (`Future<bool>`), and a null error
  /// replacement violates that type and crashes the worker.
  Future<Object?> _runHandler(
    Task task,
    TaskContext ctx,
    void Function(Future<void> settled) onAbandoned,
  ) {
    final limit = task.timeLimit;
    final future = task.handler(ctx);
    if (limit == null) return future;
    final settled = future.then<void>(
      (_) {},
      onError: (Object _, StackTrace __) {},
    );
    return future.timeout(limit, onTimeout: () {
      ctx.markAbandoned();
      onAbandoned(settled);
      throw TaskTimeLimitExceededException(ctx.id, limit);
    });
  }

  /// Reschedules [message] for [wait] later without counting it as an
  /// application-level retry, and immediately acks the current reservation
  /// — rather than blocking this concurrency slot until a rate-limit token
  /// frees up, which could otherwise leave the message unacked long enough
  /// to trip the broker's visibility-timeout redelivery.
  Future<void> _deferForRateLimit(TaskMessage message, Duration wait) async {
    final next = message.copyWith(eta: DateTime.now().add(wait));
    await app.broker.enqueue(next);
    await app.backend?.store(
      message.id,
      StoredResult(state: TaskState.scheduled, updatedAt: DateTime.now()),
    );
    _rateLimited++;
    await _emitTask(QueueEventType.taskRateLimited, next,
        fields: {'delay_ms': wait.inMilliseconds});
    app.logger.debug('task ${message.task} rate-limited, deferred '
        '${wait.inMilliseconds}ms id=${message.id}');
    await app.broker.ack(message);
  }

  Future<void> _handleRetry(
    Task task,
    TaskMessage message, {
    Duration? countdown,
    Object? cause,
  }) async {
    if (message.retries >= message.maxRetries) {
      final err = 'retries exhausted (${message.maxRetries})'
          '${cause != null ? ': $cause' : ''}';
      final result = StoredResult(
        state: TaskState.failure,
        error: err,
        updatedAt: DateTime.now(),
      );
      await _record(message, result);
      _failed++;
      await _emitTask(QueueEventType.taskFailed, message,
          fields: {'error': err});
      onFailure?.call(message, result);
      app.logger
          .error('task ${message.task} ✗ retries exhausted id=${message.id}');
      await _deadLetter(message, err);
      return;
    }
    final delay = countdown ?? task.backoffFor(message.retries + 1);
    final next = message.retried(eta: DateTime.now().add(delay));
    await app.backend?.store(
      message.id,
      StoredResult(state: TaskState.retry, updatedAt: DateTime.now()),
    );
    await app.broker.enqueue(next);
    _retried++;
    await _emitTask(QueueEventType.taskRetried, next, fields: {
      'delay_ms': delay.inMilliseconds,
      if (cause != null) 'cause': cause.toString(),
    });
    onRetry?.call(next, StoredResult(state: TaskState.retry));
    app.logger.warning('task ${message.task} ↻ retry '
        '${next.retries}/${message.maxRetries} in $delay id=${message.id}');
  }

  /// Stores [result] and, if [message] is part of a [Chain], either
  /// advances to the next step (on success) or fails every not-yet-run
  /// downstream step so a caller awaiting the chain's final id doesn't hang
  /// (on any other terminal outcome).
  Future<void> _record(TaskMessage message, StoredResult result) async {
    await app.backend?.store(message.id, result);
    final chain = message.chain;
    if (chain != null && chain.isNotEmpty) {
      if (result.state == TaskState.success) {
        await _advanceChain(message, chain, result.result);
      } else {
        await _abortChain(message, chain, result);
      }
    }
    final chordJson = message.chord;
    if (chordJson != null) {
      final barrier = ChordBarrier.fromJson(chordJson);
      if (result.state == TaskState.success) {
        await _arriveAtChord(message, barrier);
      } else {
        await _abortChord(message, barrier, result);
      }
    }
  }

  /// Called when one member of a [Chord] finishes successfully. Only the
  /// member whose arrival completes the barrier actually fires the
  /// callback — see [ResultBackend.chordArrive].
  Future<void> _arriveAtChord(TaskMessage message, ChordBarrier barrier) async {
    final backend = app.backend;
    if (backend == null) return; // chord() already guards this at apply()
    final isLast =
        await backend.chordArrive(barrier.chordId, barrier.memberIds.length);
    if (!isLast) return;
    final results = <Object?>[];
    for (final id in barrier.memberIds) {
      final snap = await backend.fetch(id);
      results.add(snap?.result);
    }
    await app.send<Object?>(
      barrier.callback.taskName,
      id: barrier.callbackId,
      args: [results, ...barrier.callback.args],
      kwargs: barrier.callback.kwargs,
      queue: barrier.callback.queue,
      maxRetries: barrier.callback.maxRetries,
    );
    app.logger.debug('chord ${barrier.chordId} complete → '
        '${barrier.callback.taskName}(${barrier.callbackId})');
  }

  /// Called when one member of a [Chord] fails or is revoked. Aborts the
  /// whole chord exactly once — see [ResultBackend.chordAbort] — so the
  /// callback's [AsyncResult] fails fast instead of hanging while it waits
  /// for a barrier that can now never legitimately complete.
  Future<void> _abortChord(
    TaskMessage message,
    ChordBarrier barrier,
    StoredResult upstream,
  ) async {
    final backend = app.backend;
    if (backend == null) return;
    final isFirst = await backend.chordAbort(barrier.chordId);
    if (!isFirst) return;
    final reason = 'chord aborted: member task "${message.task}" '
        '${upstream.state.name}${upstream.error != null ? ' (${upstream.error})' : ''}';
    await backend.store(
      barrier.callbackId,
      StoredResult(
          state: TaskState.failure, error: reason, updatedAt: DateTime.now()),
    );
    app.logger.warning('chord ${barrier.chordId} aborted: $reason');
  }

  Future<void> _advanceChain(
    TaskMessage message,
    List<Map<String, Object?>> chain,
    Object? previousResult,
  ) async {
    final link = ChainLink.fromJson(chain.first);
    final rest = chain.skip(1).toList();
    final sig = link.signature.withLeadingArg(previousResult);
    await app.send<Object?>(
      sig.taskName,
      id: link.id,
      args: sig.args,
      kwargs: sig.kwargs,
      queue: sig.queue,
      maxRetries: sig.maxRetries,
      chain: rest.isEmpty ? null : rest,
    );
    app.logger.debug(
        'chain advanced ${message.task}(${message.id}) → ${sig.taskName}(${link.id})');
  }

  Future<void> _abortChain(
    TaskMessage message,
    List<Map<String, Object?>> chain,
    StoredResult upstream,
  ) async {
    final reason = 'chain aborted: upstream task "${message.task}" '
        '${upstream.state.name}${upstream.error != null ? ' (${upstream.error})' : ''}';
    for (final raw in chain) {
      final link = ChainLink.fromJson(raw);
      await app.backend?.store(
        link.id,
        StoredResult(
            state: TaskState.failure, error: reason, updatedAt: DateTime.now()),
      );
    }
    app.logger
        .warning('chain aborted at ${message.task}(${message.id}): $reason');
  }
}
