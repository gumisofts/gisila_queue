import 'event.dart';

/// A destination for [QueueEvent]s emitted by producers and workers.
///
/// The whole observability layer is opt-in: [GisilaQueue] defaults to a
/// [NoopEventSink], so existing code pays nothing. Pass a real sink
/// ([InMemoryEventBus] for a single isolate, `RedisEventSink` for a cluster)
/// to start streaming lifecycle events to a [GisilaRose] dashboard.
///
/// Named `QueueEventSink` (not `EventSink`) to avoid clashing with
/// `dart:async`'s `EventSink<T>` in consumer code.
abstract class QueueEventSink {
  /// Publishes [event]. Implementations must never throw into the caller —
  /// observability must not break task processing; swallow/log transport
  /// errors internally instead.
  Future<void> emit(QueueEvent event);

  /// Releases any resources / connections held by the sink.
  Future<void> close() async {}
}

/// Discards every event. The zero-overhead default.
class NoopEventSink implements QueueEventSink {
  const NoopEventSink();

  @override
  Future<void> emit(QueueEvent event) async {}

  @override
  Future<void> close() async {}
}

/// Fans an event out to several sinks (e.g. a local [InMemoryEventBus] for
/// in-process dashboards plus a `RedisEventSink` for remote ones). A failing
/// sink never blocks the others.
class MultiEventSink implements QueueEventSink {
  MultiEventSink(this.sinks);

  final List<QueueEventSink> sinks;

  @override
  Future<void> emit(QueueEvent event) async {
    await Future.wait([
      for (final s in sinks) s.emit(event),
    ]);
  }

  @override
  Future<void> close() async {
    await Future.wait([for (final s in sinks) s.close()]);
  }
}

/// A readable stream of [QueueEvent]s for a monitor to consume. Mirrors the
/// write side ([QueueEventSink]); [InMemoryEventBus] implements both, while
/// Redis splits them into [RedisEventSink] (write) and [RedisEventSource]
/// (read).
abstract class QueueEventSource {
  /// A broadcast stream of every event observed from now on.
  Stream<QueueEvent> events();

  /// Recent events buffered before [events] was subscribed, oldest first.
  /// Used to backfill a freshly-started dashboard. Empty when unsupported.
  Future<List<QueueEvent>> history({int limit = 500}) async => const [];

  Future<void> close() async {}
}
