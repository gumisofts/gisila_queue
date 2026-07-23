import 'dart:async';
import 'dart:io';

/// Wires `SIGINT`/`SIGTERM` to [onShutdown], calling it at most once. Every
/// gisila_queue CLI binary ([WorkerCli], [BeatCli], `bin/rose.dart`) uses
/// this so Ctrl-C / a process manager's stop signal drains in-flight work
/// before exiting instead of killing the process mid-task.
void installShutdownHooks(Future<void> Function() onShutdown) {
  var shuttingDown = false;
  final subs = <StreamSubscription<ProcessSignal>>[];

  Future<void> handle(ProcessSignal signal) async {
    if (shuttingDown) return;
    shuttingDown = true;
    // Cancel the signal watchers themselves — otherwise their underlying
    // sockets keep the event loop (and thus the process) alive even after
    // onShutdown() has closed every broker/backend connection.
    for (final sub in subs) {
      unawaited(sub.cancel());
    }
    await onShutdown();
  }

  subs.add(ProcessSignal.sigint.watch().listen(handle));
  try {
    subs.add(ProcessSignal.sigterm.watch().listen(handle));
  } catch (_) {
    // SIGTERM isn't available on Windows; ignore.
  }
}
