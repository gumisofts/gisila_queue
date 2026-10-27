import 'dart:async';

import 'package:gisila_queue/gisila_queue.dart';
import 'package:test/test.dart';

GisilaQueue _app() => GisilaQueue(
      broker: InMemoryBroker(pollInterval: const Duration(milliseconds: 5)),
      backend: InMemoryResultBackend(),
      logger: SilentQueueLogger(),
    );

void main() {
  group('Schedule.cron (Q-11)', () {
    // 2026-10-26 is a Monday.
    DateTime at(int day, int hour, int minute) =>
        DateTime(2026, 10, day, hour, minute);

    test('steps, ranges and weekdays', () {
      final s = CronSchedule('*/15 9-17 * * 1-5');
      expect(s.matches(at(26, 9, 15)), isTrue);
      expect(s.matches(at(26, 9, 16)), isFalse);
      expect(s.matches(at(26, 18, 0)), isFalse);
      expect(s.matches(at(31, 10, 0)), isFalse, reason: 'Saturday');
    });

    test('day of month and weekday combine with OR, Sunday is 0 or 7', () {
      final s = CronSchedule('0 12 1 * 1');
      expect(s.matches(DateTime(2026, 11, 1, 12)), isTrue, reason: '1st');
      expect(s.matches(at(26, 12, 0)), isTrue, reason: 'Monday');
      expect(s.matches(at(27, 12, 0)), isFalse);
      expect(CronSchedule('0 0 * * 7').matches(DateTime(2026, 11, 1)), isTrue);
    });

    test('due once per matching minute', () {
      final s = Schedule.cron('30 3 * * *');
      final t = at(26, 3, 30);
      expect(s.isDue(null, t.add(const Duration(seconds: 5))), isTrue);
      expect(
          s.isDue(t.add(const Duration(seconds: 5)),
              t.add(const Duration(seconds: 40))),
          isFalse);
      expect(s.isDue(t, at(27, 3, 30)), isTrue);
      expect(s.isDue(null, at(26, 3, 31)), isFalse);
    });

    test('malformed expressions are refused', () {
      for (final e in [
        '* * * *',
        '60 * * * *',
        '*/0 * * * *',
        'x * * * *',
        '5-1 * * * *'
      ]) {
        expect(() => CronSchedule(e), throwsFormatException, reason: e);
      }
    });
  });

  group('dedupe keys (Q-11)', () {
    test('a second send with the same key returns the first id', () async {
      final app = _app();
      var runs = 0;
      app.task<void>('charge', (ctx) async {
        runs++;
        return null;
      });
      final a = await app.send<void>('charge', dedupeKey: 'order-7');
      final b = await app.send<void>('charge', dedupeKey: 'order-7');
      final c = await app.send<void>('charge', dedupeKey: 'order-8');
      expect(b.id, a.id);
      expect(c.id, isNot(a.id));
      final worker = app.worker()..run();
      await a.get(timeout: const Duration(seconds: 2));
      await c.get(timeout: const Duration(seconds: 2));
      await Future<void>.delayed(const Duration(milliseconds: 100));
      expect(runs, 2);
      await worker.stop();
      await app.close();
    });

    test('the key frees up after dedupeFor', () async {
      final app = _app();
      app.task<void>('t', (ctx) async => null);
      final a = await app.send<void>('t',
          dedupeKey: 'k', dedupeFor: const Duration(milliseconds: 50));
      await Future<void>.delayed(const Duration(milliseconds: 80));
      final b = await app.send<void>('t', dedupeKey: 'k');
      expect(b.id, isNot(a.id));
      await app.close();
    });

    test('needs a result backend', () async {
      final app =
          GisilaQueue(broker: InMemoryBroker(), logger: SilentQueueLogger());
      await expectLater(app.send<void>('t', dedupeKey: 'k'), throwsStateError);
      await app.close();
    });
  });

  test('stop(timeout:) does not wait forever on a stuck task (Q-11)', () async {
    final app = _app();
    final stuck = Completer<void>();
    app.task<void>('stuck', (ctx) => stuck.future);
    final worker = app.worker()..run();
    await app.send<void>('stuck');
    await Future<void>.delayed(const Duration(milliseconds: 100));
    await worker
        .stop(timeout: const Duration(milliseconds: 200))
        .timeout(const Duration(seconds: 3));
    stuck.complete();
    await app.close();
  });
}
