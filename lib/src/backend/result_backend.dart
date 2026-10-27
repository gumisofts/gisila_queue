import '../codec.dart';
import '../task_state.dart';

/// A stored snapshot of a task's outcome.
class StoredResult {
  StoredResult({
    required this.state,
    this.result,
    this.error,
    this.updatedAt,
  });

  final TaskState state;

  /// The handler's return value on success (JSON-encodable).
  final Object? result;

  /// The failure message on [TaskState.failure].
  final String? error;

  final DateTime? updatedAt;

  Map<String, Object?> toJson() => {
        'state': state.name,
        if (result != null) 'result': result,
        if (error != null) 'error': error,
        if (updatedAt != null)
          'updated_at': updatedAt!.toUtc().toIso8601String(),
      };

  factory StoredResult.fromJson(Map<String, Object?> json) => StoredResult(
        state: TaskState.parse(json['state'] as String?),
        result: json['result'],
        error: json['error'] as String?,
        updatedAt: json['updated_at'] is String
            ? DateTime.parse(json['updated_at']! as String).toLocal()
            : null,
      );

  String encode({MessageCodec codec = defaultMessageCodec}) =>
      codec.encode(toJson());

  factory StoredResult.decode(
    String raw, {
    MessageCodec codec = defaultMessageCodec,
  }) =>
      StoredResult.fromJson(codec.decode(raw));
}

/// Stores task states and return values so producers can await outcomes via
/// [AsyncResult]. Implementations: [InMemoryResultBackend] and
/// `RedisResultBackend`.
///
/// A null backend is also valid — pass none to [GisilaQueue] for pure
/// fire-and-forget processing where results are never read back.
abstract class ResultBackend {
  Future<void> store(String taskId, StoredResult result);

  Future<StoredResult?> fetch(String taskId);

  /// Drops any stored state for [taskId].
  Future<void> forget(String taskId);

  Future<void> close() async {}

  // ── Chord barrier primitives ──────────────────────────────────────────
  //
  // A `chord` (see `lib/src/canvas.dart`) needs a way for whichever worker
  // finishes the *last* group member to notice that and be the one to fire
  // the callback, while any member *failing* should abort the chord
  // exactly once no matter how many other members also fail. These two
  // primitives are all a chord needs from a backend; the default
  // implementations below use plain instance state, which is correct for
  // single-isolate use ([InMemoryResultBackend]) since nothing here ever
  // awaits between reading and writing that state. Cross-process backends
  // (see `RedisResultBackend`) override them with atomic server-side
  // primitives instead.
  final Map<String, int> _chordArrivals = {};
  final Set<String> _abortedChords = {};

  /// Records one more of a chord's [total] members finishing under
  /// [chordId]. Returns true for exactly one caller — whichever arrival
  /// brings the count to [total] — telling it (and only it) to fire the
  /// callback. Always returns false once [chordAbort] has fired for this
  /// [chordId].
  Future<bool> chordArrive(String chordId, int total) async {
    if (_abortedChords.contains(chordId)) return false;
    final n = (_chordArrivals[chordId] ?? 0) + 1;
    if (n >= total) {
      _chordArrivals.remove(chordId);
      return !_abortedChords.contains(chordId);
    }
    _chordArrivals[chordId] = n;
    return false;
  }

  /// Marks [chordId] as aborted (a member failed) and returns true for
  /// exactly the first caller — telling it (and only it) to record the
  /// chord's callback as failed. Every subsequent call for the same
  /// [chordId] (whether from another failed member or a stray success)
  /// returns false.
  Future<bool> chordAbort(String chordId) async {
    if (!_abortedChords.add(chordId)) return false;
    _chordArrivals.remove(chordId);
    return true;
  }

  // ── Idempotency keys ──────────────────────────────────────────────────

  final Map<String, (String, DateTime)> _claims = {};

  /// Claims [key] for [taskId] for [ttl]. Returns null when the claim was
  /// made, or the id that already holds [key]. Backs
  /// `GisilaQueue.send(dedupeKey:)`. The default keeps claims in this
  /// instance (right for a single process); [RedisResultBackend] makes them
  /// shared and atomic.
  Future<String?> claimKey(String key, String taskId, Duration ttl) async {
    final now = DateTime.now();
    _claims.removeWhere((_, v) => !v.$2.isAfter(now));
    final held = _claims[key];
    if (held != null) return held.$1;
    _claims[key] = (taskId, now.add(ttl));
    return null;
  }
}
