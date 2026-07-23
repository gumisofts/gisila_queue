import 'dart:async';
import 'dart:collection';

import 'event.dart';
import 'event_sink.dart';

/// In-process [EventSink] + [EventSource] backed by a broadcast stream.
///
/// Pairs with [InMemoryBroker] for single-isolate setups: a [GisilaRose]
/// dashboard embedded in the same process can subscribe directly without
/// Redis. Retains a bounded ring buffer of recent events so a dashboard that
/// attaches late still sees backfill via [history].
class InMemoryEventBus implements QueueEventSink, QueueEventSource {
  InMemoryEventBus({this.historySize = 1000});

  /// How many recent events to retain for [history] backfill.
  final int historySize;

  final StreamController<QueueEvent> _controller =
      StreamController<QueueEvent>.broadcast();
  final Queue<QueueEvent> _recent = Queue<QueueEvent>();

  @override
  Future<void> emit(QueueEvent event) async {
    _recent.addLast(event);
    while (_recent.length > historySize) {
      _recent.removeFirst();
    }
    if (_controller.hasListener) _controller.add(event);
  }

  @override
  Stream<QueueEvent> events() => _controller.stream;

  @override
  Future<List<QueueEvent>> history({int limit = 500}) async {
    final list = _recent.toList();
    if (list.length <= limit) return list;
    return list.sublist(list.length - limit);
  }

  @override
  Future<void> close() async {
    await _controller.close();
  }
}
