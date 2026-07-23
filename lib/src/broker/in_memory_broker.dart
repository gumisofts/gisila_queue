import 'dart:async';
import 'dart:collection';

import '../message.dart';
import 'broker.dart';

/// In-process [Broker] backed by plain Dart collections.
///
/// Producers and workers must live in the same isolate. Ideal for tests,
/// local development, and single-process apps that just want background
/// execution without standing up Redis. Delivery is at-most-once: a message
/// reserved by a worker is removed immediately.
class InMemoryBroker extends Broker {
  final Map<String, Queue<TaskMessage>> _queues = {};
  final List<TaskMessage> _scheduled = [];

  /// How often [reserve] re-checks the queues while waiting.
  final Duration pollInterval;

  InMemoryBroker({this.pollInterval = const Duration(milliseconds: 25)});

  Queue<TaskMessage> _queue(String name) =>
      _queues.putIfAbsent(name, Queue<TaskMessage>.new);

  @override
  Future<void> enqueue(TaskMessage message) async {
    if (message.isDelayed(DateTime.now())) {
      _scheduled.add(message);
    } else {
      _queue(message.queue).add(message);
    }
  }

  void _promoteDue() {
    if (_scheduled.isEmpty) return;
    final now = DateTime.now();
    _scheduled.removeWhere((m) {
      if (m.isDelayed(now)) return false;
      _queue(m.queue).add(m);
      return true;
    });
  }

  TaskMessage? _tryReserve(List<String> queues) {
    _promoteDue();
    for (final name in queues) {
      final q = _queues[name];
      if (q != null && q.isNotEmpty) return q.removeFirst();
    }
    return null;
  }

  @override
  Future<TaskMessage?> reserve(List<String> queues,
      {required Duration timeout}) async {
    final deadline = DateTime.now().add(timeout);
    while (true) {
      final msg = _tryReserve(queues);
      if (msg != null) return msg;
      if (!DateTime.now().isBefore(deadline)) return null;
      await Future<void>.delayed(pollInterval);
    }
  }

  @override
  Future<int> length(String queue) async => _queues[queue]?.length ?? 0;

  @override
  Future<int> purge(String queue) async {
    final q = _queues[queue];
    if (q == null) return 0;
    final n = q.length;
    q.clear();
    return n;
  }

  /// Total messages held back for a future ETA across all queues.
  int get scheduledCount => _scheduled.length;
}
