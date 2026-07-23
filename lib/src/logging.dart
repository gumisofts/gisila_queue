import 'dart:io';

/// Severity levels for [QueueLogger].
enum LogLevel { debug, info, warning, error }

/// Minimal logging seam so the package pulls in no logging dependency.
///
/// Swap in your own implementation (forwarding to `package:logging`,
/// `logger`, or the Gisila web logger) via [GisilaQueue]'s constructor.
abstract class QueueLogger {
  void log(LogLevel level, String message, {Object? error, StackTrace? stack});

  void debug(String m) => log(LogLevel.debug, m);
  void info(String m) => log(LogLevel.info, m);
  void warning(String m) => log(LogLevel.warning, m);
  void error(String m, {Object? error, StackTrace? stack}) =>
      log(LogLevel.error, m, error: error, stack: stack);
}

/// Writes single-line, level-prefixed records to stderr.
class ConsoleQueueLogger extends QueueLogger {
  ConsoleQueueLogger({this.minLevel = LogLevel.info});

  final LogLevel minLevel;

  @override
  void log(LogLevel level, String message, {Object? error, StackTrace? stack}) {
    if (level.index < minLevel.index) return;
    final tag = level.name.toUpperCase().padRight(7);
    stderr.writeln('[gisila_queue] $tag $message');
    if (error != null) stderr.writeln('  cause: $error');
    if (stack != null && level == LogLevel.error) stderr.writeln(stack);
  }
}

/// Drops every record; handy in tests.
class SilentQueueLogger extends QueueLogger {
  @override
  void log(LogLevel level, String message,
      {Object? error, StackTrace? stack}) {}
}
