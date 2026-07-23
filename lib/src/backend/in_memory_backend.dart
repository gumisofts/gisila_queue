import 'dart:collection';

import 'result_backend.dart';

/// In-process [ResultBackend] backed by a [Map]. Pairs with
/// [InMemoryBroker] for single-isolate setups and tests.
///
/// Bounded like [MonitorState]'s `maxTasks`: [maxEntries] caps how many
/// results are retained (oldest-touched evicted first), and [ttl] — when
/// set — makes [fetch] treat entries older than [ttl] as gone, mirroring
/// Celery's `result_expires`. Both default to unbounded/no-expiry, matching
/// this backend's previous behaviour.
class InMemoryResultBackend extends ResultBackend {
  InMemoryResultBackend({this.maxEntries, this.ttl});

  /// Upper bound on retained results; oldest-touched are evicted past this.
  /// Null (the default) means unbounded.
  final int? maxEntries;

  /// How long a stored result stays fetchable after its last [store] before
  /// [fetch] starts treating it as absent (still occupies a slot toward
  /// [maxEntries] until a later [store]/[_evictExpired] sweep drops it).
  /// Null (the default) means results never expire on their own.
  final Duration? ttl;

  // Insertion-ordered so eviction drops the oldest-touched entry.
  final LinkedHashMap<String, StoredResult> _store = LinkedHashMap();

  @override
  Future<void> store(String taskId, StoredResult result) async {
    // Re-insert at the end to mark as most-recently touched.
    _store.remove(taskId);
    _store[taskId] = result;
    final max = maxEntries;
    if (max != null) {
      while (_store.length > max) {
        _store.remove(_store.keys.first);
      }
    }
  }

  @override
  Future<StoredResult?> fetch(String taskId) async {
    final result = _store[taskId];
    if (result == null) return null;
    if (_isExpired(result)) {
      _store.remove(taskId);
      return null;
    }
    return result;
  }

  @override
  Future<void> forget(String taskId) async {
    _store.remove(taskId);
  }

  bool _isExpired(StoredResult result) {
    final ttl = this.ttl;
    final updatedAt = result.updatedAt;
    if (ttl == null || updatedAt == null) return false;
    return DateTime.now().difference(updatedAt) > ttl;
  }
}
