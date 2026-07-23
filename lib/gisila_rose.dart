/// GisilaRose — an optional, Flower-style observability dashboard for
/// gisila_queue.
///
/// This is a **separate entry point** from `package:gisila_queue/gisila_queue.dart`
/// so the core stays dependency-light: importing it pulls in the `dart:io`
/// HTTP server only when you actually want a dashboard.
///
/// Wire a [QueueEventSink] into your [GisilaQueue] (producers and workers) so
/// lifecycle events flow, then run a [GisilaRose] against the matching
/// [QueueEventSource]:
///
/// ```dart
/// // In producer/worker processes:
/// final app = GisilaQueue(
///   broker: RedisBroker(host: 'localhost'),
///   events: RedisEventSink(host: 'localhost'),
/// );
///
/// // In the monitor process (or see bin/rose.dart):
/// final rose = GisilaRose(
///   source: RedisEventSource(host: 'localhost'),
///   broker: RedisBroker(host: 'localhost'),
///   port: 5555,
/// );
/// await rose.start();
/// print('dashboard on http://localhost:5555');
/// ```
///
/// For a single-isolate app or tests, share one [InMemoryEventBus] as both the
/// app's `events` sink and the rose's `source`.
library;

export 'src/monitor/monitor_state.dart';
export 'src/monitor/rose.dart';
