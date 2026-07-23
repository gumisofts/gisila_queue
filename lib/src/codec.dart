import 'dart:convert';

/// Serializes a [TaskMessage]/[StoredResult] JSON envelope to and from the
/// wire format brokers and result backends actually transmit.
///
/// Defaults to compact JSON everywhere (see [JsonMessageCodec]) — swap in an
/// alternate implementation (e.g. msgpack, a compressing codec) to change
/// the wire format without touching broker/backend code.
abstract class MessageCodec {
  String encode(Map<String, Object?> json);
  Map<String, Object?> decode(String raw);
}

/// The default [MessageCodec]: plain `dart:convert` JSON.
class JsonMessageCodec implements MessageCodec {
  const JsonMessageCodec();

  @override
  String encode(Map<String, Object?> json) => jsonEncode(json);

  @override
  Map<String, Object?> decode(String raw) =>
      jsonDecode(raw) as Map<String, Object?>;
}

/// The codec every [TaskMessage]/[StoredResult] uses unless a call site is
/// updated to accept its own — a single top-level default rather than a
/// per-instance field, since every producer and every worker in a cluster
/// must agree on the wire format regardless of which object created a
/// particular envelope.
const MessageCodec defaultMessageCodec = JsonMessageCodec();

/// Thrown at the producer call site ([TaskRef.delay]/[TaskRef.applyAsync]/
/// [GisilaQueue.send]) when [args]/[kwargs] contain a value [MessageCodec]
/// can't serialize — e.g. a `DateTime`, a custom class, or a cycle. Raised
/// eagerly so the failure points at the `.delay()` call instead of surfacing
/// deep inside a broker's `enqueue`.
class UnencodableArgumentsException implements Exception {
  const UnencodableArgumentsException(this.taskName, this.cause);

  final String taskName;
  final Object cause;

  @override
  String toString() =>
      'UnencodableArgumentsException: args/kwargs for "$taskName" are not '
      'JSON-encodable ($cause). Only JSON-compatible values (num, String, '
      'bool, null, List, Map) may be passed as task arguments.';
}

/// Eagerly validates that [args]/[kwargs] round-trip through [codec],
/// throwing [UnencodableArgumentsException] for [taskName] if they don't.
/// Called by [GisilaQueue.send] before a message is built, so producers get
/// a clear, immediate error instead of one buried inside broker internals.
void validateEncodable(
  String taskName,
  List<Object?> args,
  Map<String, Object?> kwargs, {
  MessageCodec codec = defaultMessageCodec,
}) {
  try {
    codec.encode(<String, Object?>{'args': args, 'kwargs': kwargs});
  } catch (e) {
    throw UnencodableArgumentsException(taskName, e);
  }
}
