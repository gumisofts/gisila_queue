import 'package:gisila_queue/gisila_queue.dart';
import 'package:test/test.dart';

void main() {
  group('IntervalSchedule (Schedule.every)', () {
    test('is due immediately when it has never fired', () {
      final s = Schedule.every(const Duration(minutes: 5));
      expect(s.isDue(null, DateTime.now()), isTrue);
    });

    test('is not due before the period has elapsed', () {
      final s = Schedule.every(const Duration(minutes: 5));
      final last = DateTime(2026, 1, 1, 12, 0);
      expect(s.isDue(last, last.add(const Duration(minutes: 4))), isFalse);
    });

    test('is due exactly at and after the period', () {
      final s = Schedule.every(const Duration(minutes: 5));
      final last = DateTime(2026, 1, 1, 12, 0);
      expect(s.isDue(last, last.add(const Duration(minutes: 5))), isTrue);
      expect(s.isDue(last, last.add(const Duration(minutes: 6))), isTrue);
    });
  });

  group('DailySchedule (Schedule.dailyAt)', () {
    test('is not due before today\'s fire time', () {
      final s = Schedule.dailyAt(9, minute: 30);
      final now = DateTime(2026, 3, 15, 9, 29);
      expect(s.isDue(null, now), isFalse);
    });

    test('is due at or after today\'s fire time when never run', () {
      final s = Schedule.dailyAt(9, minute: 30);
      expect(s.isDue(null, DateTime(2026, 3, 15, 9, 30)), isTrue);
      expect(s.isDue(null, DateTime(2026, 3, 15, 14, 0)), isTrue);
    });

    test('is not due again the same day once already run today', () {
      final s = Schedule.dailyAt(9, minute: 30);
      final ranAt = DateTime(2026, 3, 15, 9, 30);
      expect(s.isDue(ranAt, DateTime(2026, 3, 15, 18, 0)), isFalse);
    });

    test('fires again the next day past the fire time', () {
      final s = Schedule.dailyAt(9, minute: 30);
      final ranAt = DateTime(2026, 3, 15, 9, 30);
      expect(s.isDue(ranAt, DateTime(2026, 3, 16, 9, 30)), isTrue);
    });

    test('defaults minute to 0', () {
      final s = Schedule.dailyAt(8) as DailySchedule;
      expect(s.hour, 8);
      expect(s.minute, 0);
    });
  });
}
