import 'dart:async';
import 'dart:collection' as collection;

import 'package:dart_amqp/dart_amqp.dart';

import '../message.dart';
import 'broker.dart';

/// Distributed [Broker] backed by RabbitMQ (AMQP 0-9-1) via `dart_amqp`.
///
/// Topology (namespaced by [prefix]):
///
/// * Direct exchange `<prefix>.tasks` routes ready work to durable queues
///   `<prefix>.queue.<name>` (binding key = logical queue name).
/// * Fanout exchange `<prefix>.delay` feeds a durable `<prefix>.scheduled`
///   queue configured with `x-dead-letter-exchange` → `<prefix>.tasks`.
///   Delayed (ETA) messages are published there with a per-message
///   `expiration` (TTL). When the TTL elapses RabbitMQ dead-letters the
///   message onto the tasks exchange using the original routing key (the
///   logical queue name), so it lands on the correct work queue.
///
/// Delivery is at-least-once: [reserve] hands out a manually-acked AMQP
/// delivery, and [ack] confirms it. Unacked messages (worker crash, or
/// [close] while in flight) are requeued by the broker.
///
/// `dart_amqp` has no `basicGet`, so [reserve] bridges push consumers into
/// a priority-aware in-memory inbox — higher-priority queues in the
/// [reserve] list are drained first.
class RabbitBroker extends Broker {
  RabbitBroker({
    this.host = 'localhost',
    this.port = 5672,
    this.virtualHost = '/',
    this.username = 'guest',
    this.password = 'guest',
    this.prefix = 'gq',
    this.prefetchCount = 32,
  });

  final String host;
  final int port;
  final String virtualHost;
  final String username;
  final String password;

  /// Namespace for exchanges and queues on a shared vhost.
  final String prefix;

  /// AMQP basic.qos prefetch — how many unacked deliveries RabbitMQ may
  /// push to this process at once.
  final int prefetchCount;

  Client? _client;
  Channel? _channel;
  Exchange? _tasksExchange;
  Exchange? _delayExchange;
  Future<void>? _connecting;
  bool _closed = false;

  final Set<String> _declaredQueues = {};
  final Map<String, Consumer> _consumers = {};
  final Map<String, StreamSubscription<AmqpMessage>> _subscriptions = {};

  /// Ready deliveries waiting to be handed out by [reserve], keyed by
  /// logical queue name.
  final Map<String, collection.Queue<_Delivery>> _inbox = {};

  /// Reserved but not yet [ack]ed — keyed by [TaskMessage.id].
  final Map<String, AmqpMessage> _inFlight = {};

  /// Waiters blocked inside [reserve] until a delivery arrives (or timeout).
  final List<Completer<void>> _waiters = [];

  String get _tasksExchangeName => '$prefix.tasks';
  String get _delayExchangeName => '$prefix.delay';
  String get _scheduledQueueName => '$prefix.scheduled';
  String _queueName(String logical) => '$prefix.queue.$logical';

  Future<void> _ensureConnected() {
    if (_closed) {
      return Future.error(StateError('RabbitBroker is closed'));
    }
    if (_channel != null && _tasksExchange != null && _delayExchange != null) {
      return Future.value();
    }
    return _connecting ??= _connect();
  }

  Future<void> _connect() async {
    try {
      final client = Client(
        settings: ConnectionSettings(
          host: host,
          port: port,
          virtualHost: virtualHost,
          authProvider: PlainAuthenticator(username, password),
          maxConnectionAttempts: 5,
          reconnectWaitTime: const Duration(milliseconds: 500),
        ),
      );
      final channel = await client.channel();
      await channel.qos(0, prefetchCount);

      final tasks = await channel.exchange(
        _tasksExchangeName,
        ExchangeType.DIRECT,
        durable: true,
      );
      final delay = await channel.exchange(
        _delayExchangeName,
        ExchangeType.FANOUT,
        durable: true,
      );
      final scheduled = await channel.queue(
        _scheduledQueueName,
        durable: true,
        arguments: <String, Object>{
          'x-dead-letter-exchange': _tasksExchangeName,
        },
      );
      // Fanout ignores the binding key; empty is allowed for FANOUT.
      await scheduled.bind(delay, '');

      _client = client;
      _channel = channel;
      _tasksExchange = tasks;
      _delayExchange = delay;
    } finally {
      _connecting = null;
    }
  }

  Future<void> _ensureWorkQueue(String logical) async {
    await _ensureConnected();
    if (_declaredQueues.contains(logical)) return;
    final channel = _channel!;
    final queue = await channel.queue(_queueName(logical), durable: true);
    await queue.bind(_tasksExchange!, logical);
    _declaredQueues.add(logical);
  }

  Future<void> _ensureConsumers(List<String> queues) async {
    for (final logical in queues) {
      await _ensureWorkQueue(logical);
      if (_consumers.containsKey(logical)) continue;
      final channel = _channel!;
      final queue = await channel.queue(_queueName(logical), durable: true);
      final consumer = await queue.consume(noAck: false);
      _consumers[logical] = consumer;
      _subscriptions[logical] = consumer.listen(
        (amqp) => _onDelivery(logical, amqp),
        onError: (_) {},
      );
    }
  }

  void _onDelivery(String logical, AmqpMessage amqp) {
    if (_closed) {
      try {
        amqp.reject(true);
      } catch (_) {}
      return;
    }
    TaskMessage message;
    try {
      message = TaskMessage.decode(amqp.payloadAsString);
    } catch (_) {
      // Malformed payload — drop so it can't poison the consumer.
      try {
        amqp.reject(false);
      } catch (_) {}
      return;
    }
    _inbox
        .putIfAbsent(logical, collection.Queue<_Delivery>.new)
        .add(_Delivery(message: message, amqp: amqp));
    _notifyWaiters();
  }

  void _notifyWaiters() {
    if (_waiters.isEmpty) return;
    final waiters = List<Completer<void>>.from(_waiters);
    _waiters.clear();
    for (final c in waiters) {
      if (!c.isCompleted) c.complete();
    }
  }

  _Delivery? _tryTake(List<String> queues) {
    for (final logical in queues) {
      final q = _inbox[logical];
      if (q != null && q.isNotEmpty) return q.removeFirst();
    }
    return null;
  }

  MessageProperties _persistentProps(TaskMessage message) {
    final props = MessageProperties.persistentMessage();
    props.messageId = message.id;
    props.contentType = 'application/json';
    props.contentEncoding = 'utf-8';
    return props;
  }

  @override
  Future<void> enqueue(TaskMessage message) async {
    await _ensureWorkQueue(message.queue);
    final payload = message.encode();
    final now = DateTime.now();
    if (message.isDelayed(now)) {
      final delayMs = message.eta!.difference(now).inMilliseconds;
      if (delayMs <= 0) {
        _tasksExchange!.publish(
          payload,
          message.queue,
          properties: _persistentProps(message),
        );
        return;
      }
      // Routing key is ignored by the fanout delay exchange for routing, but
      // RabbitMQ preserves it for dead-lettering onto the tasks exchange.
      final props = _persistentProps(message)..expiration = '$delayMs';
      _delayExchange!.publish(payload, message.queue, properties: props);
    } else {
      _tasksExchange!.publish(
        payload,
        message.queue,
        properties: _persistentProps(message),
      );
    }
  }

  @override
  Future<TaskMessage?> reserve(List<String> queues,
      {required Duration timeout}) async {
    if (queues.isEmpty) return null;
    await _ensureConsumers(queues);

    final deadline = DateTime.now().add(timeout);
    while (!_closed) {
      final taken = _tryTake(queues);
      if (taken != null) {
        _inFlight[taken.message.id] = taken.amqp;
        return taken.message;
      }

      final remaining = deadline.difference(DateTime.now());
      if (remaining <= Duration.zero) return null;

      final waiter = Completer<void>();
      _waiters.add(waiter);

      // Delivery may have arrived between the take and waiter registration.
      final raced = _tryTake(queues);
      if (raced != null) {
        _waiters.remove(waiter);
        if (!waiter.isCompleted) waiter.complete();
        _inFlight[raced.message.id] = raced.amqp;
        return raced.message;
      }

      try {
        await waiter.future.timeout(remaining);
      } on TimeoutException {
        _waiters.remove(waiter);
        final last = _tryTake(queues);
        if (last == null) return null;
        _inFlight[last.message.id] = last.amqp;
        return last.message;
      } finally {
        _waiters.remove(waiter);
      }
    }
    return null;
  }

  @override
  Future<void> ack(TaskMessage message) async {
    final amqp = _inFlight.remove(message.id);
    if (amqp == null) return;
    try {
      amqp.ack();
    } catch (_) {
      // Channel may already be closed during shutdown.
    }
  }

  @override
  Future<int> length(String queue) async {
    await _ensureWorkQueue(queue);
    final q = await _channel!.queue(_queueName(queue), durable: true);
    return q.messageCount;
  }

  @override
  Future<int> purge(String queue) async {
    await _ensureWorkQueue(queue);
    // Also drop any locally buffered deliveries for this queue so length/
    // subsequent reserves stay consistent with the broker.
    final buffered = _inbox.remove(queue);
    var local = 0;
    if (buffered != null) {
      local = buffered.length;
      while (buffered.isNotEmpty) {
        final d = buffered.removeFirst();
        try {
          d.amqp.reject(false);
        } catch (_) {}
      }
    }
    final q = await _channel!.queue(_queueName(queue), durable: true);
    final before = q.messageCount;
    await q.purge();
    // messageCount at declare time; purge return isn't exposed as a count
    // on this client — prefer the pre-purge broker depth + local drops.
    return before + local;
  }

  /// Deletes this broker's exchanges and queues under [prefix]. Intended for
  /// integration-test cleanup — never call against a shared production prefix.
  Future<void> deleteTopology() async {
    await _ensureConnected();
    final channel = _channel!;
    for (final logical in List<String>.from(_declaredQueues)) {
      try {
        final q = await channel.queue(_queueName(logical), durable: true);
        await q.delete();
      } catch (_) {}
    }
    _declaredQueues.clear();
    try {
      final scheduled =
          await channel.queue(_scheduledQueueName, durable: true, arguments: {
        'x-dead-letter-exchange': _tasksExchangeName,
      });
      await scheduled.delete();
    } catch (_) {}
    try {
      await (await channel.exchange(
        _delayExchangeName,
        ExchangeType.FANOUT,
        durable: true,
      ))
          .delete();
    } catch (_) {}
    try {
      await (await channel.exchange(
        _tasksExchangeName,
        ExchangeType.DIRECT,
        durable: true,
      ))
          .delete();
    } catch (_) {}
    _tasksExchange = null;
    _delayExchange = null;
  }

  @override
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    _notifyWaiters();

    for (final sub in _subscriptions.values) {
      await sub.cancel();
    }
    _subscriptions.clear();

    for (final consumer in _consumers.values) {
      try {
        await consumer.cancel();
      } catch (_) {}
    }
    _consumers.clear();

    // Requeue anything still in the inbox or reserved but unacked so another
    // worker (or a restarted process) can pick it up.
    for (final q in _inbox.values) {
      while (q.isNotEmpty) {
        final d = q.removeFirst();
        try {
          d.amqp.reject(true);
        } catch (_) {}
      }
    }
    _inbox.clear();

    for (final amqp in _inFlight.values) {
      try {
        amqp.reject(true);
      } catch (_) {}
    }
    _inFlight.clear();

    try {
      await _channel?.close();
    } catch (_) {}
    try {
      await _client?.close();
    } catch (_) {}
    _channel = null;
    _client = null;
    _tasksExchange = null;
    _delayExchange = null;
    _declaredQueues.clear();
  }
}

class _Delivery {
  _Delivery({required this.message, required this.amqp});

  final TaskMessage message;
  final AmqpMessage amqp;
}
