import '../message.dart';

/// Transport that moves [TaskMessage]s from producers to workers.
///
/// A broker is responsible for three things:
///
/// * **Enqueue** a message onto a named queue, honouring [TaskMessage.eta]
///   by holding delayed messages until they are due.
/// * **Reserve** the next ready message for a worker, blocking up to a
///   timeout so idle workers don't busy-loop.
/// * **Acknowledge** a reserved message once it has been fully handled,
///   enabling at-least-once delivery on backends that support it.
///
/// Implementations: [InMemoryBroker] (single-process / tests),
/// `RedisBroker` (distributed), and `RabbitBroker` (AMQP / RabbitMQ).
abstract class Broker {
  /// Places [message] on its queue. If the message carries a future ETA it
  /// is held until due, then promoted automatically by [reserve].
  Future<void> enqueue(TaskMessage message);

  /// Blocks for up to [timeout] waiting for a ready message on any of
  /// [queues] (checked in priority order). Returns null on timeout.
  Future<TaskMessage?> reserve(List<String> queues,
      {required Duration timeout});

  /// Marks a reserved [message] as fully processed. No-op for fire-and-forget
  /// list brokers; meaningful for reliable-queue implementations.
  Future<void> ack(TaskMessage message) async {}

  /// Number of ready (non-delayed) messages waiting on [queue].
  Future<int> length(String queue);

  /// Drops every ready message waiting on [queue] without running them —
  /// Celery/`celery purge`'s equivalent. Returns how many were discarded.
  /// Delayed (ETA/scheduled) messages destined for [queue] are untouched.
  Future<int> purge(String queue);

  /// Releases any resources / connections held by the broker.
  Future<void> close() async {}
}
