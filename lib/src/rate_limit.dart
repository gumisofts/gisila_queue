import 'dart:collection';

/// Caps how often a task may run, e.g. `RateLimit.parse('10/s')`.
///
/// Mirrors Celery's `rate_limit` string format (`'100/m'`, `'10/s'`,
/// `'1000/h'`) and, like Celery's default rate-limit implementation, is
/// enforced **per worker process** rather than cluster-wide — running more
/// worker processes scales total throughput past a single rate limit, same
/// as Celery.
class RateLimit {
  const RateLimit(this.count, this.per);

  /// Maximum number of runs allowed within [per].
  final int count;

  /// The sliding window [count] applies to.
  final Duration per;

  static final _pattern = RegExp(r'^\s*(\d+)\s*/\s*(s|m|h)\s*$');

  /// Parses Celery-style shorthand: `'<n>/s'`, `'<n>/m'`, or `'<n>/h'`.
  factory RateLimit.parse(String spec) {
    final match = _pattern.firstMatch(spec);
    if (match == null) {
      throw FormatException(
          'invalid rate limit "$spec" — expected e.g. "10/s", "100/m", "1000/h"');
    }
    final count = int.parse(match.group(1)!);
    final per = switch (match.group(2)!) {
      's' => const Duration(seconds: 1),
      'm' => const Duration(minutes: 1),
      'h' => const Duration(hours: 1),
      _ => const Duration(seconds: 1),
    };
    return RateLimit(count, per);
  }

  @override
  String toString() => '$count/${per.inSeconds}s';
}

/// A sliding-window limiter: at most [RateLimit.count] acquisitions inside
/// any [RateLimit.per]-long trailing window.
///
/// Deliberately not a blocking primitive — [tryAcquire] returns immediately
/// with either "go ahead" (null) or "here's how long until you can", so
/// callers can reschedule work instead of parking a worker slot.
class SlidingWindowLimiter {
  SlidingWindowLimiter(this.limit);

  final RateLimit limit;
  final Queue<DateTime> _recent = Queue<DateTime>();

  /// If a slot is free right now, consumes it and returns null. Otherwise
  /// returns the [Duration] to wait before retrying.
  Duration? tryAcquire([DateTime? now]) {
    final at = now ?? DateTime.now();
    while (_recent.isNotEmpty && at.difference(_recent.first) >= limit.per) {
      _recent.removeFirst();
    }
    if (_recent.length < limit.count) {
      _recent.addLast(at);
      return null;
    }
    final freeAt = _recent.first.add(limit.per);
    final wait = freeAt.difference(at);
    return wait.isNegative ? Duration.zero : wait;
  }
}
