import 'dart:async';
import 'dart:io';

import '../app.dart';
import '../namespace.dart';
import 'coordination.dart';
import 'schedule.dart';

/// A registered periodic task: a [name], the [taskName] to enqueue, the
/// arguments to enqueue it with, and the [schedule] governing when.
class BeatEntry {
  BeatEntry({
    required this.name,
    required this.taskName,
    required this.schedule,
    this.args = const [],
    this.kwargs = const {},
    this.queue,
  });

  final String name;
  final String taskName;
  final Schedule schedule;
  final List<Object?> args;
  final Map<String, Object?> kwargs;
  final String? queue;

  DateTime? lastRun;
}

/// A lightweight periodic-task scheduler, the equivalent of `celery beat`.
///
/// It owns a list of [BeatEntry]s and, on each [tick] (driven by a timer in
/// [run]), enqueues any that are due via the app. Celery's docs say to run
/// exactly one beat process per cluster; pass a [lock] (e.g.
/// [RedisBeatLock]) to make that enforced rather than just assumed — a
/// second beat process then sits idle instead of double-firing every entry.
/// Pass a [stateStore] (e.g. [RedisBeatStateStore]) so a beat restart
/// remembers each entry's last-fired time instead of refiring it
/// immediately.
class Beat {
  Beat(
    this.app, {
    this.tickInterval = const Duration(seconds: 1),
    BeatLock? lock,
    BeatStateStore? stateStore,
    this.lockTtl = const Duration(seconds: 30),
    String? id,
  })  : lock = lock ?? app.beatLock ?? const NoopBeatLock(),
        stateStore =
            stateStore ?? app.beatStateStore ?? const NoopBeatStateStore(),
        id = id ?? '${_hostname()}:$pid' {
    namespace = checkNamespaces({
      'app': app.namespace,
      'lock': this.lock,
      'stateStore': this.stateStore,
    });
  }

  final GisilaQueue app;

  /// How often the schedule is evaluated.
  final Duration tickInterval;

  /// Distributed mutex held while this instance is the active scheduler.
  /// Defaults to [GisilaQueue.beatLock], else [NoopBeatLock] (always
  /// "leader"), matching the previous single-instance-by-convention
  /// behaviour.
  final BeatLock lock;

  /// Where each entry's last-fired time is persisted across restarts.
  /// Defaults to [GisilaQueue.beatStateStore], else [NoopBeatStateStore]
  /// (nothing persisted).
  final BeatStateStore stateStore;

  /// The prefix shared by [app], [lock] and [stateStore], or null when none
  /// is namespaced. The constructor throws [ArgumentError] when they differ.
  late final String? namespace;

  /// How long a lock acquisition is held for before it must be renewed.
  final Duration lockTtl;

  /// Stable identity used as the lock owner.
  final String id;

  final List<BeatEntry> _entries = [];
  Timer? _timer;
  bool _running = false;
  bool _isLeader = false;
  bool _stateLoaded = false;
  bool _ticking = false;

  static String _hostname() {
    try {
      return Platform.localHostname;
    } catch (_) {
      return 'beat';
    }
  }

  /// Registers a periodic [entry]. Returns the same entry for chaining.
  BeatEntry register(BeatEntry entry) {
    _entries.add(entry);
    return entry;
  }

  /// Convenience to register a task to run on a fixed [period].
  BeatEntry schedule(
    String name,
    String taskName,
    Schedule schedule, {
    List<Object?> args = const [],
    Map<String, Object?> kwargs = const {},
    String? queue,
  }) {
    return register(BeatEntry(
      name: name,
      taskName: taskName,
      schedule: schedule,
      args: args,
      kwargs: kwargs,
      queue: queue,
    ));
  }

  /// Brings each entry's [BeatEntry.lastRun] up to the persisted value.
  /// Run once at startup and again whenever this instance (re)gains
  /// leadership: while it stood by, the leader kept firing and persisting,
  /// so its own last-run times are stale and would refire everything.
  Future<void> _loadPersistedState() async {
    for (final entry in _entries) {
      final persisted = await stateStore.getLastRun(entry.name);
      if (persisted != null &&
          (entry.lastRun == null || persisted.isAfter(entry.lastRun!))) {
        entry.lastRun = persisted;
      }
    }
    _stateLoaded = true;
  }

  /// Attempts to become (or remain) leader. Returns whether this instance
  /// currently holds the lock.
  Future<bool> _ensureLeadership() async {
    try {
      _isLeader = await lock.acquire(id, lockTtl);
    } catch (e, st) {
      app.logger.error('beat lock acquire failed', error: e, stack: st);
      _isLeader = false;
    }
    return _isLeader;
  }

  /// Evaluates every entry once and enqueues those that are due. Exposed
  /// separately from [run] so tests can drive ticks deterministically.
  /// A no-op whenever another instance currently holds [lock].
  Future<void> tick([DateTime? at]) async {
    // A slow tick (Redis latency, a slow send) must not overlap the next.
    if (_ticking) return;
    _ticking = true;
    try {
      await _tick(at);
    } finally {
      _ticking = false;
    }
  }

  Future<void> _tick(DateTime? at) async {
    final wasLeader = _isLeader;
    if (!await _ensureLeadership()) return;
    if (!wasLeader || !_stateLoaded) await _loadPersistedState();
    final now = at ?? DateTime.now();
    for (final entry in _entries) {
      if (!entry.schedule.isDue(entry.lastRun, now)) continue;
      entry.lastRun = now;
      try {
        await app.send<Object?>(
          entry.taskName,
          args: entry.args,
          kwargs: entry.kwargs,
          queue: entry.queue,
        );
        await stateStore.setLastRun(entry.name, now);
        app.logger.debug('beat fired ${entry.name} → ${entry.taskName}');
      } catch (e, st) {
        app.logger
            .error('beat failed to enqueue ${entry.name}', error: e, stack: st);
      }
    }
  }

  /// Starts the periodic timer; resolves when [stop] is called.
  Future<void> run() async {
    if (_running) return;
    _running = true;
    app.logger.info('beat started: ${_entries.length} entries (id=$id)');
    if (namespace == defaultNamespace) {
      app.logger.warning(defaultNamespaceWarning('beat $id'));
    }
    final completer = Completer<void>();
    _timer = Timer.periodic(tickInterval, (_) async {
      if (!_running) return;
      await tick();
    });
    _stopSignal = completer;
    await completer.future;
  }

  Completer<void>? _stopSignal;

  /// Stops the scheduler, releases [lock] if held, and closes [lock]'s and
  /// [stateStore]'s own connections (e.g. [RedisBeatLock]/
  /// [RedisBeatStateStore] sockets) — callers still own and must close
  /// [app] separately.
  Future<void> stop() async {
    _running = false;
    _timer?.cancel();
    _timer = null;
    if (_isLeader) {
      _isLeader = false;
      try {
        await lock.release(id);
      } catch (_) {
        // Best-effort — the TTL will expire it regardless.
      }
    }
    await lock.close();
    await stateStore.close();
    if (_stopSignal != null && !_stopSignal!.isCompleted) {
      _stopSignal!.complete();
    }
  }
}
