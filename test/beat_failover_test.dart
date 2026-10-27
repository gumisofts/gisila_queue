import 'dart:async';

import 'package:gisila_queue/gisila_queue.dart';
import 'package:test/test.dart';

/// One lock shared by every [Beat] in a test, like a Redis key would be.
class _SharedLock implements BeatLock {
  String? owner;

  @override
  Future<bool> acquire(String who, Duration ttl) async {
    owner ??= who;
    return owner == who;
  }

  @override
  Future<void> release(String who) async {
    if (owner == who) owner = null;
  }

  @override
  Future<void> close() async {}
}

class _SharedState implements BeatStateStore {
  final Map<String, DateTime> lastRun = {};
  Duration delay = Duration.zero;

  @override
  Future<DateTime?> getLastRun(String name) async {
    await Future<void>.delayed(delay);
    return lastRun[name];
  }

  @override
  Future<void> setLastRun(String name, DateTime at) async => lastRun[name] = at;

  @override
  Future<void> close() async {}
}

void main() {
  late GisilaQueue app;

  setUp(() {
    app = GisilaQueue(broker: InMemoryBroker(), logger: SilentQueueLogger());
  });
  tearDown(() => app.close());

  Beat beat(String id, BeatLock lock, BeatStateStore state) =>
      Beat(app, lock: lock, stateStore: state, id: id)
        ..schedule('every', 'job', Schedule.every(const Duration(minutes: 1)));

  Future<int> queued() async {
    var n = 0;
    while (await app.broker.reserve([app.defaultQueue],
            timeout: const Duration(milliseconds: 20)) !=
        null) {
      n++;
    }
    return n;
  }

  test('a standby that takes over reloads what the leader last fired',
      () async {
    final lock = _SharedLock();
    final state = _SharedState();
    final a = beat('a', lock, state);
    final b = beat('b', lock, state);
    final t0 = DateTime(2026, 1, 1, 12);

    await a.tick(t0); // a leads and fires
    await b.tick(t0.add(const Duration(seconds: 30))); // b stands by
    await a.tick(t0.add(const Duration(minutes: 1))); // a fires again
    expect(await queued(), 2);

    // a dies; b takes over 10s after a's last run. Before, b still had the
    // last-run time it read while standing by (t0) and fired at once.
    lock.owner = null;
    await b.tick(t0.add(const Duration(minutes: 1, seconds: 10)));
    expect(await queued(), 0);
    await b.tick(t0.add(const Duration(minutes: 2)));
    expect(await queued(), 1);
  });

  test('ticks do not overlap, so none runs before the state is loaded',
      () async {
    final t = DateTime(2026, 1, 1, 12);
    final state = _SharedState()
      ..lastRun['every'] = t
      ..delay = const Duration(milliseconds: 50);
    final b = beat('b', _SharedLock(), state);
    // Before, the second tick skipped the still-loading state and fired.
    final at = t.add(const Duration(seconds: 10));
    await Future.wait([b.tick(at), b.tick(at), b.tick(at)]);
    expect(await queued(), 0);
  });
}
