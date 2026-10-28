import 'package:uuid/uuid.dart';

import 'backend/redis_backend.dart';
import 'backend/result_backend.dart';
import 'beat/coordination.dart';
import 'broker/broker.dart';
import 'broker/redis_broker.dart';
import 'canvas.dart';
import 'codec.dart';
import 'control.dart';
import 'events/event.dart';
import 'events/event_sink.dart';
import 'events/redis_event_sink.dart';
import 'logging.dart';
import 'message.dart';
import 'namespace.dart';
import 'rate_limit.dart';
import 'redis_options.dart';
import 'result.dart';
import 'router.dart';
import 'task.dart';
import 'task_state.dart';
import 'worker/worker.dart';

/// The central application object, analogous to a Celery `Celery()` app.
///
/// It owns the task registry, the [Broker], and the optional
/// [ResultBackend], and is the single thing both producers and workers
/// construct. Register tasks with [task]; enqueue them through the returned
/// [TaskRef]; process them by building a [worker].
///
/// ```dart
/// final app = GisilaQueue(
///   broker: RedisBroker(host: 'localhost'),
///   backend: RedisResultBackend(host: 'localhost'),
/// );
///
/// final add = app.task<int>('math.add', (ctx) async =>
///     (ctx.arg(0) as int) + (ctx.arg(1) as int));
///
/// // producer
/// final r = await add.delay([2, 3]);
/// print(await r.get()); // 5  (once a worker has run)
///
/// // worker process
/// await app.worker().run();
/// ```
///
/// Apps that share a Redis database or RabbitMQ vhost need their own
/// namespace (every component's `prefix`); [GisilaQueue.redis] sets it once
/// for all of them. The constructor throws [ArgumentError] when the
/// components passed to it use different prefixes.
class GisilaQueue {
  GisilaQueue({
    required this.broker,
    this.backend,
    this.defaultQueue = 'default',
    QueueEventSink? events,
    QueueLogger? logger,
    Uuid? idGenerator,
    ControlChannel? controlChannel,
    this.router,
    this.beatLock,
    this.beatStateStore,
  })  : events = events ?? const NoopEventSink(),
        logger = logger ?? ConsoleQueueLogger(),
        _uuid = idGenerator ?? const Uuid(),
        controlChannel = controlChannel ?? const NoopControlChannel(),
        namespace = checkNamespaces({
          'broker': broker,
          'backend': backend,
          'events': events,
          'controlChannel': controlChannel,
          'beatLock': beatLock,
          'beatStateStore': beatStateStore,
        });

  /// A Redis-backed app whose broker, result backend, control channel,
  /// event sink (with [events]), beat lock and beat state store all use
  /// [namespace] as their prefix. Give each app on a shared Redis its own
  /// [namespace]; workers, producers and beat of one app must agree on it.
  factory GisilaQueue.redis({
    required String namespace,
    String host = 'localhost',
    int port = 6379,
    RedisOptions options = const RedisOptions(),
    bool results = true,
    bool control = true,
    bool events = false,
    Duration visibilityTimeout = const Duration(minutes: 5),
    Duration resultTtl = const Duration(hours: 24),
    String defaultQueue = 'default',
    QueueLogger? logger,
    Uuid? idGenerator,
    Router? router,
  }) {
    if (namespace.isEmpty || namespace.contains(RegExp(r'\s'))) {
      throw ArgumentError.value(
          namespace, 'namespace', 'must be non-empty, without spaces');
    }
    return GisilaQueue(
      broker: RedisBroker(
          host: host,
          port: port,
          options: options,
          prefix: namespace,
          visibilityTimeout: visibilityTimeout),
      backend: results
          ? RedisResultBackend(
              host: host,
              port: port,
              options: options,
              prefix: namespace,
              resultTtl: resultTtl)
          : null,
      controlChannel: control
          ? RedisControlChannel(
              host: host, port: port, options: options, prefix: namespace)
          : null,
      events: events
          ? RedisEventSink(
              host: host, port: port, options: options, prefix: namespace)
          : null,
      beatLock: RedisBeatLock(
          host: host, port: port, options: options, prefix: namespace),
      beatStateStore: RedisBeatStateStore(
          host: host, port: port, options: options, prefix: namespace),
      defaultQueue: defaultQueue,
      logger: logger,
      idGenerator: idGenerator,
      router: router,
    );
  }

  /// The prefix shared by this app's namespaced components (Redis or
  /// RabbitMQ), or null when none is namespaced (e.g. all in-memory).
  final String? namespace;

  /// Lock a [Beat] built for this app uses when given none. Set by
  /// [GisilaQueue.redis].
  final BeatLock? beatLock;

  /// State store a [Beat] built for this app uses when given none. Set by
  /// [GisilaQueue.redis].
  final BeatStateStore? beatStateStore;

  /// Transport used to move messages between producers and workers.
  final Broker broker;

  /// Optional store for task states / return values. When null, tasks run
  /// fire-and-forget and [AsyncResult.get] is unavailable.
  final ResultBackend? backend;

  /// Queue used when a task / call specifies none and [router] (if any)
  /// doesn't match.
  final String defaultQueue;

  /// Optional declarative routing consulted by [send] whenever a call
  /// doesn't pin an explicit queue — see [Router].
  final Router? router;

  /// Observability sink fed lifecycle [QueueEvent]s by producers and workers.
  /// Defaults to [NoopEventSink] (zero overhead); supply an [InMemoryEventBus]
  /// or `RedisEventSink` to drive a [GisilaRose] dashboard.
  final QueueEventSink events;

  final QueueLogger logger;
  final Uuid _uuid;

  /// Broadcast channel for [AsyncResult.revoke] and [Worker] queue
  /// pause/resume. Defaults to [NoopControlChannel] (zero overhead);
  /// supply an [InMemoryControlChannel] or [RedisControlChannel] to enable
  /// it — see [ControlChannel].
  final ControlChannel controlChannel;

  final Map<String, Task> _registry = {};

  /// Registered tasks, keyed by name (read-only view).
  Map<String, Task> get registry => Map.unmodifiable(_registry);

  /// Looks up a registered task by [name], or null.
  Task? lookup(String name) => _registry[name];

  /// Registers a task [handler] under [name] and returns a typed [TaskRef]
  /// for enqueuing it. [T] is the handler's result type, surfaced through
  /// [AsyncResult].
  ///
  /// Throws [ArgumentError] if [name] is already registered.
  TaskRef<T> task<T>(
    String name,
    TaskHandler handler, {
    String? queue,
    int maxRetries = 0,
    Duration retryBackoff = const Duration(seconds: 1),
    Duration retryBackoffMax = const Duration(minutes: 10),
    bool retryJitter = true,
    bool autoRetry = false,
    Duration? timeLimit,
    Duration? softTimeLimit,
    RateLimit? rateLimit,
  }) {
    if (_registry.containsKey(name)) {
      throw ArgumentError.value(name, 'name', 'task already registered');
    }
    final t = Task(
      name: name,
      handler: handler,
      queue: queue,
      maxRetries: maxRetries,
      retryBackoff: retryBackoff,
      retryBackoffMax: retryBackoffMax,
      retryJitter: retryJitter,
      autoRetry: autoRetry,
      timeLimit: timeLimit,
      softTimeLimit: softTimeLimit,
      rateLimit: rateLimit,
    );
    _registry[name] = t;
    return TaskRef<T>(this, t);
  }

  /// Low-level enqueue used by [TaskRef]. Produces a [TaskMessage], records a
  /// pending (or scheduled) state in the backend, and hands it to the broker.
  ///
  /// [id] overrides the generated task id — used by [chain] to pre-assign
  /// every step's id up front. [chain] carries the remaining steps of a
  /// pipeline this message is part of (see [TaskMessage.chain]); leave it
  /// null for a plain, unchained call.
  ///
  /// Queue resolution, in order: an explicit [queue] always wins; failing
  /// that, [router] (if set); failing that, the task's own registered
  /// default queue; failing that, [defaultQueue].
  ///
  /// Throws [UnencodableArgumentsException] immediately if [args]/[kwargs]
  /// aren't JSON-encodable, rather than letting the failure surface later
  /// deep inside the broker's `enqueue`.
  Future<AsyncResult<T>> send<T>(
    String taskName, {
    List<Object?> args = const [],
    Map<String, Object?> kwargs = const {},
    String? queue,
    DateTime? eta,
    int maxRetries = 0,
    DateTime? expires,
    String? id,
    List<Map<String, Object?>>? chain,
    Map<String, Object?>? chord,
    String? dedupeKey,
    Duration dedupeFor = const Duration(hours: 1),
  }) async {
    validateEncodable(taskName, args, kwargs);
    final effectiveQueue = queue ??
        router?.resolve(taskName) ??
        _registry[taskName]?.queue ??
        defaultQueue;
    final message = TaskMessage(
      id: id ?? _uuid.v4(),
      task: taskName,
      queue: effectiveQueue,
      args: args,
      kwargs: kwargs,
      maxRetries: maxRetries,
      eta: eta,
      createdAt: DateTime.now(),
      expires: expires,
      chain: chain,
      chord: chord,
    );
    if (dedupeKey != null) {
      final store = backend;
      if (store == null) {
        throw StateError('send(dedupeKey:) needs a result backend.');
      }
      final existing =
          await store.claimKey('$taskName:$dedupeKey', message.id, dedupeFor);
      if (existing != null) {
        logger.debug('dedupe: $taskName key=$dedupeKey already sent as '
            '$existing; not enqueued again');
        return AsyncResult<T>(this, existing);
      }
    }
    final delayed = message.isDelayed(DateTime.now());
    await backend?.store(
      message.id,
      StoredResult(
        state: delayed ? TaskState.scheduled : TaskState.pending,
        updatedAt: DateTime.now(),
      ),
    );
    await broker.enqueue(message);
    await events.emit(QueueEvent(
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
        if (message.eta != null) 'eta': message.eta!.toUtc().toIso8601String(),
        if (message.expires != null)
          'expires': message.expires!.toUtc().toIso8601String(),
      },
    ));
    logger.debug('enqueued $taskName id=${message.id} queue=${message.queue}'
        '${delayed ? ' eta=${eta!.toIso8601String()}' : ''}');
    return AsyncResult<T>(this, message.id);
  }

  /// Returns an [AsyncResult] handle for a known task [id] (e.g. one
  /// persisted elsewhere) without enqueuing anything.
  AsyncResult<T> resultFor<T>(String id) => AsyncResult<T>(this, id);

  /// Generates a fresh unique id — e.g. so [chain] can pre-assign every
  /// step's id before enqueuing any of them.
  String newTaskId() => _uuid.v4();

  /// Builds a sequential pipeline of [signatures] — see [Chain]. Call
  /// [Chain.apply] to enqueue it; the returned [AsyncResult] resolves once
  /// the *last* step finishes.
  ///
  /// ```dart
  /// final result = await app.chain<String>([
  ///   fetchUser.sig(args: [userId]),
  ///   formatGreeting.sig(),
  /// ]).apply();
  /// ```
  Chain<T> chain<T>(List<Signature> signatures) => Chain<T>(this, signatures);

  /// Builds a parallel set of [signatures] — see [Group]. Call
  /// [Group.apply] to enqueue every member at once and get back a
  /// [GroupResult] handle for awaiting all of them.
  ///
  /// ```dart
  /// final results = await app.group<int>([
  ///   resize.sig(args: [imageId, 'small']),
  ///   resize.sig(args: [imageId, 'large']),
  /// ]).apply();
  /// await results.get();
  /// ```
  Group<T> group<T>(List<Signature> signatures) => Group<T>(this, signatures);

  /// Builds a [Group] of [signatures] plus a [callback] that fires once
  /// every member has finished — see [Chord]. Call [Chord.apply] to
  /// enqueue it; the returned [AsyncResult] resolves once the callback
  /// itself finishes, with the member results passed as its first
  /// argument.
  ///
  /// ```dart
  /// final result = await app.chord<int>(
  ///   [resize.sig(args: [imageId, 'small']), resize.sig(args: [imageId, 'large'])],
  ///   sumSizes.sig(),
  /// ).apply();
  /// ```
  Chord<T> chord<T>(List<Signature> signatures, Signature callback) =>
      Chord<T>(this, signatures, callback);

  /// Builds a [Worker] bound to this app.
  ///
  /// * [queues] — which queues to consume, in priority order; defaults to
  ///   `[defaultQueue]`.
  /// * [concurrency] — how many messages to process at once within this
  ///   process. Scale further by running more worker processes.
  /// * [deadLetterQueue], [maxRequeues], [maxRedeliveries],
  ///   [unknownTaskBackoff] — see [Worker].
  Worker worker({
    List<String>? queues,
    int concurrency = 1,
    Duration reserveTimeout = const Duration(seconds: 5),
    String? id,
    String? deadLetterQueue = Worker.defaultDeadLetterQueue,
    int maxRequeues = 10,
    int maxRedeliveries = 5,
    Duration unknownTaskBackoff = const Duration(seconds: 1),
  }) {
    return Worker(
      this,
      queues: queues ?? [defaultQueue],
      concurrency: concurrency,
      reserveTimeout: reserveTimeout,
      id: id, // null -> Worker defaults to a human-readable host:pid
      deadLetterQueue: deadLetterQueue,
      maxRequeues: maxRequeues,
      maxRedeliveries: maxRedeliveries,
      unknownTaskBackoff: unknownTaskBackoff,
    );
  }

  /// Moves up to [limit] messages from the dead-letter [queue] back to the
  /// queue each one failed on, with its retry, requeue and redelivery
  /// counters reset, and returns how many were moved. Fix whatever made them
  /// fail first, or they will end up back on [queue].
  Future<int> replayDeadLetters({
    String queue = Worker.defaultDeadLetterQueue,
    int limit = 100,
  }) async {
    var moved = 0;
    while (moved < limit) {
      final message = await broker
          .reserve([queue], timeout: const Duration(milliseconds: 200));
      if (message == null) break;
      final origin = message.deadLetter?['queue'];
      // Pending again, so an AsyncResult waits for the new run instead of
      // returning the old failure.
      await backend?.store(
        message.id,
        StoredResult(state: TaskState.pending, updatedAt: DateTime.now()),
      );
      await broker.enqueue(message.copyWith(
        queue: origin is String ? origin : defaultQueue,
        retries: 0,
        requeues: 0,
        redeliveries: 0,
        clearDeadLetter: true,
      ));
      await broker.ack(message);
      moved++;
    }
    return moved;
  }

  /// Closes the broker, backend, and event-sink connections.
  Future<void> close() async {
    await broker.close();
    await backend?.close();
    await events.close();
    await controlChannel.close();
  }
}
