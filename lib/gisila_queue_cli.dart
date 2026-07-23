/// Shared CLI scaffolding for gisila_queue worker/beat binaries.
///
/// Kept separate from `package:gisila_queue/gisila_queue.dart` (which has
/// no `dart:io` dependency beyond what the core library already needs) so
/// pulling in process-signal handling and flag parsing is opt-in — only
/// import this from an actual `bin/*.dart` entrypoint.
///
/// ```dart
/// // bin/worker.dart
/// import 'package:gisila_queue/gisila_queue.dart';
/// import 'package:gisila_queue/gisila_queue_cli.dart';
///
/// Future<void> main(List<String> argv) async {
///   final cli = WorkerCli.parse(argv);
///   if (cli.help) return print(WorkerCli.usage);
///   final app = cli.buildApp();
///   app.task<void>('emails.send_welcome', (ctx) async { /* ... */ });
///   await cli.run(app);
/// }
/// ```
library;

export 'src/cli/beat_cli.dart';
export 'src/cli/cli_args.dart';
export 'src/cli/shutdown.dart';
export 'src/cli/worker_cli.dart';
