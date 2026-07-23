import 'app.dart';
import 'result.dart';

/// An unevaluated call to a registered task — a name plus the arguments to
/// invoke it with. Mirrors Celery's `Signature`/`.si()`/`.s()`: build one
/// with [TaskRef.sig], then compose several into a [chain] or [group].
///
/// Signatures are JSON-encodable so they can travel embedded inside a
/// [chain]'s [TaskMessage.chain] payload.
class Signature {
  const Signature(
    this.taskName, {
    this.args = const [],
    this.kwargs = const {},
    this.queue,
    this.maxRetries = 0,
  });

  final String taskName;
  final List<Object?> args;
  final Map<String, Object?> kwargs;
  final String? queue;
  final int maxRetries;

  /// A copy with [value] spliced in as the new first positional argument —
  /// how [chain] feeds one step's result into the next step's call.
  Signature withLeadingArg(Object? value) => Signature(
        taskName,
        args: [value, ...args],
        kwargs: kwargs,
        queue: queue,
        maxRetries: maxRetries,
      );

  Map<String, Object?> toJson() => {
        'task': taskName,
        'args': args,
        'kwargs': kwargs,
        if (queue != null) 'queue': queue,
        'max_retries': maxRetries,
      };

  factory Signature.fromJson(Map<String, Object?> json) => Signature(
        json['task'] as String,
        args: (json['args'] as List?)?.cast<Object?>() ?? const [],
        kwargs: (json['kwargs'] as Map?)?.cast<String, Object?>() ?? const {},
        queue: json['queue'] as String?,
        maxRetries: (json['max_retries'] as num?)?.toInt() ?? 0,
      );

  @override
  String toString() => 'Signature($taskName, args: $args, kwargs: $kwargs)';
}

/// One pre-assigned step of a [Chain]: the id it will run under (assigned
/// client-side, before anything is enqueued, so the caller can get an
/// [AsyncResult] handle for every step immediately) plus its [Signature].
class ChainLink {
  const ChainLink(this.id, this.signature);

  final String id;
  final Signature signature;

  Map<String, Object?> toJson() => {'id': id, 'sig': signature.toJson()};

  factory ChainLink.fromJson(Map<String, Object?> json) => ChainLink(
        json['id'] as String,
        Signature.fromJson(json['sig'] as Map<String, Object?>),
      );
}

/// A sequential pipeline of tasks — Celery's `chain(sig1, sig2, ...)`.
///
/// Only the first step is enqueued up front; the remaining steps travel
/// with it as [TaskMessage.chain] metadata. When a step succeeds, the
/// *worker* that ran it enqueues the next step, splicing the previous
/// step's return value in as its new first positional argument. If a step
/// fails or is revoked, the whole chain is aborted and every not-yet-run
/// step (including the final one this returns an [AsyncResult] for) is
/// immediately recorded as failed, so callers awaiting the last step don't
/// hang waiting for a step that will never run.
class Chain<T> {
  Chain(this._app, this.signatures)
      : assert(signatures.isNotEmpty, 'chain needs at least one signature');

  final GisilaQueue _app;
  final List<Signature> signatures;

  /// Enqueues the whole pipeline and returns an [AsyncResult] for its
  /// *last* step — awaiting it waits for the entire chain to finish.
  Future<AsyncResult<T>> apply({
    String? queue,
    Duration? countdown,
    DateTime? eta,
  }) async {
    final links = [
      for (final sig in signatures) ChainLink(_app.newTaskId(), sig),
    ];
    final first = links.first;
    final rest = links.skip(1).map((l) => l.toJson()).toList();
    await _app.send<Object?>(
      first.signature.taskName,
      id: first.id,
      args: first.signature.args,
      kwargs: first.signature.kwargs,
      queue: queue ?? first.signature.queue,
      maxRetries: first.signature.maxRetries,
      eta: countdown != null ? DateTime.now().add(countdown) : eta,
      chain: rest.isEmpty ? null : rest,
    );
    return AsyncResult<T>(_app, links.last.id);
  }
}

/// The outcome handle for a [Group] — a fixed set of [AsyncResult]s that
/// ran (or are running) in parallel.
class GroupResult<T> {
  GroupResult(this.results);

  /// One [AsyncResult] per signature, in the same order they were given to
  /// [GisilaQueue.group].
  final List<AsyncResult<T>> results;

  /// Waits for every member to finish and returns their values in order.
  /// Propagates the first failure exactly like [AsyncResult.get] would.
  Future<List<T?>> get({
    Duration timeout = const Duration(seconds: 30),
    Duration pollInterval = const Duration(milliseconds: 100),
  }) =>
      Future.wait([
        for (final r in results)
          r.get(timeout: timeout, pollInterval: pollInterval),
      ]);

  /// Whether every member has reached a terminal state.
  Future<bool> get ready async {
    for (final r in results) {
      if (!await r.ready) return false;
    }
    return true;
  }
}

/// A set of tasks enqueued together and run in parallel — Celery's
/// `group(sig1, sig2, ...)`. Every signature is sent immediately (no
/// ordering/dependency between them); use [GroupResult.get] to await all of
/// them, or a [Chord] if you need a callback once they've all finished.
class Group<T> {
  Group(this._app, this.signatures)
      : assert(signatures.isNotEmpty, 'group needs at least one signature');

  final GisilaQueue _app;
  final List<Signature> signatures;

  /// Enqueues every signature and returns a [GroupResult] handle for all of
  /// them.
  Future<GroupResult<T>> apply() async {
    final results = await Future.wait([
      for (final sig in signatures)
        _app.send<T>(
          sig.taskName,
          args: sig.args,
          kwargs: sig.kwargs,
          queue: sig.queue,
          maxRetries: sig.maxRetries,
        ),
    ]);
    return GroupResult<T>(results);
  }
}

/// The barrier metadata a [Chord] attaches to every group member message
/// (`TaskMessage.chord`), so whichever worker processes the last member
/// knows to fire [callback] with every member's result.
class ChordBarrier {
  const ChordBarrier({
    required this.chordId,
    required this.memberIds,
    required this.callbackId,
    required this.callback,
  });

  final String chordId;
  final List<String> memberIds;
  final String callbackId;
  final Signature callback;

  Map<String, Object?> toJson() => {
        'chord_id': chordId,
        'member_ids': memberIds,
        'callback_id': callbackId,
        'callback': callback.toJson(),
      };

  factory ChordBarrier.fromJson(Map<String, Object?> json) => ChordBarrier(
        chordId: json['chord_id'] as String,
        memberIds: (json['member_ids'] as List).cast<String>(),
        callbackId: json['callback_id'] as String,
        callback: Signature.fromJson(json['callback'] as Map<String, Object?>),
      );
}

/// A [Group] plus a [callback] that runs exactly once, after every member
/// has finished — Celery's `chord(group(...), callback)`. The callback
/// receives the list of member results (in the same order the signatures
/// were given) spliced in as its first positional argument.
///
/// If any member fails (or is revoked), the chord is aborted: the
/// callback's [AsyncResult] is immediately recorded as failed instead of
/// waiting on members that would otherwise never let the barrier complete.
///
/// Requires a [ResultBackend] on the [GisilaQueue] — the barrier and each
/// member's result both live there.
class Chord<T> {
  Chord(this._app, this.signatures, this.callback)
      : assert(signatures.isNotEmpty, 'chord needs at least one signature');

  final GisilaQueue _app;
  final List<Signature> signatures;
  final Signature callback;

  /// Enqueues every member (tagged with the shared [ChordBarrier]) and
  /// returns an [AsyncResult] for [callback] — awaiting it waits for the
  /// whole chord.
  Future<AsyncResult<T>> apply() async {
    if (_app.backend == null) {
      throw StateError(
          'chord() requires a ResultBackend (GisilaQueue(backend: ...)) to '
          'track its member barrier and callback result.');
    }
    final chordId = _app.newTaskId();
    final callbackId = _app.newTaskId();
    final memberIds = [for (final _ in signatures) _app.newTaskId()];
    final barrier = ChordBarrier(
      chordId: chordId,
      memberIds: memberIds,
      callbackId: callbackId,
      callback: callback,
    ).toJson();
    await Future.wait([
      for (var i = 0; i < signatures.length; i++)
        _app.send<Object?>(
          signatures[i].taskName,
          id: memberIds[i],
          args: signatures[i].args,
          kwargs: signatures[i].kwargs,
          queue: signatures[i].queue,
          maxRetries: signatures[i].maxRetries,
          chord: barrier,
        ),
    ]);
    return AsyncResult<T>(_app, callbackId);
  }
}
