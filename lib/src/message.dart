import 'codec.dart';

/// The wire envelope that travels through the broker for a single task
/// invocation. Equivalent to a Celery message body: it carries the task
/// name plus its positional [args] and keyword [kwargs], along with the
/// routing and retry metadata a worker needs to execute it.
class TaskMessage {
  TaskMessage({
    required this.id,
    required this.task,
    required this.queue,
    this.args = const [],
    this.kwargs = const {},
    this.retries = 0,
    this.maxRetries = 0,
    this.eta,
    this.createdAt,
    this.redeliveries = 0,
    this.expires,
    this.chain,
    this.chord,
  });

  /// Unique id; doubles as the result-backend key.
  final String id;

  /// Registered task name, e.g. `emails.send_welcome`.
  final String task;

  /// Destination queue name.
  final String queue;

  /// Positional arguments passed to the handler.
  final List<Object?> args;

  /// Keyword arguments passed to the handler.
  final Map<String, Object?> kwargs;

  /// How many times this message has already been retried.
  final int retries;

  /// Upper bound on retries before the task is marked failed.
  final int maxRetries;

  /// Earliest time the task may run. Null means "as soon as possible".
  final DateTime? eta;

  /// When the message was first produced.
  final DateTime? createdAt;

  /// How many times a broker has redelivered this exact message because it
  /// was reserved but never acked (e.g. the worker holding it crashed).
  /// Distinct from [retries], which counts application-level retry attempts
  /// requested by task logic.
  final int redeliveries;

  /// The latest time this task may still usefully run. Once past, a worker
  /// records it as [TaskState.revoked] instead of executing it. Null (the
  /// default) means it never expires.
  final DateTime? expires;

  /// Whether the ETA lies in the future relative to [now].
  bool isDelayed(DateTime now) => eta != null && eta!.isAfter(now);

  /// Whether [expires] lies in the past relative to [now].
  bool isExpired(DateTime now) => expires != null && expires!.isBefore(now);

  /// Remaining steps of a [Chain] this message is part of, each a
  /// `{'id': ..., 'sig': Signature.toJson()}` map (see [ChainLink]). Null
  /// or empty means this message isn't chained to anything further. Kept
  /// as raw JSON here (rather than typed [ChainLink]s) so `message.dart`
  /// doesn't need to depend on `canvas.dart`.
  final List<Map<String, Object?>>? chain;

  /// If this message is one member of a [Chord]'s group, the barrier
  /// metadata for it (see `ChordBarrier.toJson`) — kept as a raw JSON map
  /// for the same reason as [chain]. Null for a message that isn't part of
  /// a chord.
  final Map<String, Object?>? chord;

  /// A copy advanced to the next retry attempt, due at [eta].
  TaskMessage retried({required DateTime eta}) => copyWith(
        retries: retries + 1,
        eta: eta,
        clearEta: false,
      );

  /// A copy marking one more broker-level redelivery (see [redeliveries]).
  TaskMessage redelivered() => copyWith(redeliveries: redeliveries + 1);

  TaskMessage copyWith({
    String? queue,
    int? retries,
    int? maxRetries,
    DateTime? eta,
    bool clearEta = false,
    int? redeliveries,
    DateTime? expires,
    List<Map<String, Object?>>? chain,
    Map<String, Object?>? chord,
  }) {
    return TaskMessage(
      id: id,
      task: task,
      queue: queue ?? this.queue,
      args: args,
      kwargs: kwargs,
      retries: retries ?? this.retries,
      maxRetries: maxRetries ?? this.maxRetries,
      eta: clearEta ? null : (eta ?? this.eta),
      createdAt: createdAt,
      redeliveries: redeliveries ?? this.redeliveries,
      expires: expires ?? this.expires,
      chain: chain ?? this.chain,
      chord: chord ?? this.chord,
    );
  }

  Map<String, Object?> toJson() => {
        'id': id,
        'task': task,
        'queue': queue,
        'args': args,
        'kwargs': kwargs,
        'retries': retries,
        'max_retries': maxRetries,
        if (eta != null) 'eta': eta!.toUtc().toIso8601String(),
        if (createdAt != null)
          'created_at': createdAt!.toUtc().toIso8601String(),
        if (redeliveries != 0) 'redeliveries': redeliveries,
        if (expires != null) 'expires': expires!.toUtc().toIso8601String(),
        if (chain != null && chain!.isNotEmpty) 'chain': chain,
        if (chord != null) 'chord': chord,
      };

  factory TaskMessage.fromJson(Map<String, Object?> json) {
    DateTime? parseTime(Object? v) =>
        v is String ? DateTime.parse(v).toLocal() : null;
    return TaskMessage(
      id: json['id'] as String,
      task: json['task'] as String,
      queue: json['queue'] as String,
      args: (json['args'] as List?)?.cast<Object?>() ?? const [],
      kwargs: (json['kwargs'] as Map?)?.cast<String, Object?>() ?? const {},
      retries: (json['retries'] as num?)?.toInt() ?? 0,
      maxRetries: (json['max_retries'] as num?)?.toInt() ?? 0,
      eta: parseTime(json['eta']),
      createdAt: parseTime(json['created_at']),
      redeliveries: (json['redeliveries'] as num?)?.toInt() ?? 0,
      expires: parseTime(json['expires']),
      chain: (json['chain'] as List?)
          ?.map((e) => (e as Map).cast<String, Object?>())
          .toList(),
      chord: (json['chord'] as Map?)?.cast<String, Object?>(),
    );
  }

  /// Serializes to a wire string using [codec] (defaults to
  /// [defaultMessageCodec], plain JSON).
  String encode({MessageCodec codec = defaultMessageCodec}) =>
      codec.encode(toJson());

  /// Parses a [TaskMessage] from its [encode]d form, using [codec].
  factory TaskMessage.decode(
    String raw, {
    MessageCodec codec = defaultMessageCodec,
  }) =>
      TaskMessage.fromJson(codec.decode(raw));

  @override
  String toString() =>
      'TaskMessage($task id=$id queue=$queue retries=$retries)';
}
