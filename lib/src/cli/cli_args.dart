/// Minimal `--flag` / `--flag=value` parser shared by gisila_queue's CLI
/// helpers ([WorkerCli], [BeatCli]) and `bin/rose.dart`, so every
/// gisila_queue binary parses arguments the same way.
class CliArgs {
  CliArgs(Map<String, String> values) : _values = Map.unmodifiable(values);

  final Map<String, String> _values;

  factory CliArgs.parse(List<String> argv) {
    final out = <String, String>{};
    for (final a in argv) {
      if (!a.startsWith('--')) continue;
      final body = a.substring(2);
      final eq = body.indexOf('=');
      if (eq == -1) {
        out[body] = 'true';
      } else {
        out[body.substring(0, eq)] = body.substring(eq + 1);
      }
    }
    return CliArgs(out);
  }

  bool has(String key) => _values.containsKey(key);

  String? string(String key) => _values[key];

  String stringOr(String key, String fallback) => _values[key] ?? fallback;

  int intOr(String key, int fallback) =>
      int.tryParse(_values[key] ?? '') ?? fallback;

  /// A comma-separated flag (e.g. `--queues=emails,default`) split and
  /// trimmed into a list, or null if the flag wasn't given.
  List<String>? stringList(String key) {
    final raw = _values[key];
    if (raw == null) return null;
    return raw
        .split(',')
        .map((s) => s.trim())
        .where((s) => s.isNotEmpty)
        .toList();
  }
}
