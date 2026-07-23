/// Declarative task → queue routing, Celery's `task_routes` equivalent.
///
/// Rules are consulted in the order they were added; the first match wins.
/// Attach one to [GisilaQueue.router] so `.delay()`/`.applyAsync()` calls
/// that don't specify a queue explicitly get routed automatically instead
/// of everything landing on the default queue (or requiring every call
/// site to pass `queue: ...` by hand).
///
/// ```dart
/// final router = Router()
///   ..route('emails.*', queue: 'emails')
///   ..route('reports.*', queue: 'low-priority');
/// final app = GisilaQueue(broker: ..., router: router);
/// ```
class Router {
  final List<_Rule> _rules = [];

  /// Routes any task name matching [glob] to [queue]. `*` matches any
  /// sequence of characters (e.g. `'emails.*'` matches `'emails.send'`);
  /// every other character is matched literally. For anything more precise
  /// (anchors, character classes, etc.), use [routeMatching] instead.
  void route(String glob, {required String queue}) {
    _rules.add(_Rule(_globToRegExp(glob), queue));
  }

  /// Routes any task name matching [pattern] to [queue].
  void routeMatching(RegExp pattern, {required String queue}) {
    _rules.add(_Rule(pattern, queue));
  }

  /// The queue for [taskName] per the first matching rule, or null if
  /// nothing matches (callers should fall back to a task-level or app-level
  /// default queue in that case).
  String? resolve(String taskName) {
    for (final rule in _rules) {
      if (rule.pattern.hasMatch(taskName)) return rule.queue;
    }
    return null;
  }

  static RegExp _globToRegExp(String glob) {
    final escaped = RegExp.escape(glob).replaceAll(r'\*', '.*');
    return RegExp('^$escaped\$');
  }
}

class _Rule {
  _Rule(this.pattern, this.queue);
  final RegExp pattern;
  final String queue;
}
