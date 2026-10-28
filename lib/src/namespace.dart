/// A component that keeps its keys, queues or channels under a namespace
/// [prefix] on a server other apps may share: the Redis and RabbitMQ
/// brokers, the Redis result backend, control channel, event sink/source,
/// beat lock and beat state store.
///
/// Two apps on the same Redis database or RabbitMQ vhost must use different
/// prefixes, or their workers take each other's tasks and only one of their
/// Beats ever runs. Every component of one app must use the same prefix;
/// [GisilaQueue] and [Beat] refuse a mix (see [checkNamespaces]).
abstract interface class Namespaced {
  /// The namespace every key, queue and channel name starts with.
  String get prefix;
}

/// The prefix every component uses when none is given. Fine for one app per
/// Redis database or vhost; a worker or beat on it logs a warning.
const defaultNamespace = 'gq';

/// Returns the one prefix shared by the [Namespaced] values (and plain
/// `String` prefixes) in [components], null when there are none (e.g. all
/// in-memory), or throws [ArgumentError] naming each one when they
/// disagree.
String? checkNamespaces(Map<String, Object?> components) {
  final prefixes = <String, String>{
    for (final e in components.entries)
      if (e.value case Namespaced(:final prefix) || final String prefix)
        e.key: prefix,
  };
  final distinct = prefixes.values.toSet();
  if (distinct.length > 1) {
    final list = prefixes.entries.map((e) => '${e.key}="${e.value}"');
    throw ArgumentError(
        'gisila_queue components use different namespaces (${list.join(', ')}); '
        'give every component of one app the same prefix, '
        'or build them with GisilaQueue.redis(namespace: ...)');
  }
  return distinct.isEmpty ? null : distinct.single;
}

/// The warning a worker or beat logs when it runs on [defaultNamespace].
String defaultNamespaceWarning(String who) =>
    '$who is using the default namespace "$defaultNamespace": another app on '
    'the same Redis database or RabbitMQ vhost with the default would share '
    "its queues (its workers take this app's tasks) and its beat lock. "
    'Set a per-app prefix, e.g. GisilaQueue.redis(namespace: "myapp").';
