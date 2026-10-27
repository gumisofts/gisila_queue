/// Decides when a periodic entry is next due. Implementations are checked
/// by the [Beat] on each tick.
abstract class Schedule {
  /// Given the [last] time the entry fired (null if never) and the current
  /// time [now], returns true when the entry should fire.
  bool isDue(DateTime? last, DateTime now);

  /// A fixed-period schedule firing every [period].
  factory Schedule.every(Duration period) = IntervalSchedule;

  /// A daily schedule firing at [hour]:[minute] (local time).
  factory Schedule.dailyAt(int hour, {int minute}) = DailySchedule;

  /// A cron schedule (local time): `minute hour day-of-month month
  /// day-of-week`, each `*`, a number, a range `a-b`, a list `a,b` or a
  /// step `*/n` / `a-b/n`. Sunday is 0 (or 7). As in cron, when both day
  /// fields are restricted a day matching either one fires.
  ///
  /// ```dart
  /// Schedule.cron('*/15 9-17 * * 1-5'); // every 15 min, office hours
  /// ```
  factory Schedule.cron(String expression) = CronSchedule;
}

/// Fires on a fixed interval.
class IntervalSchedule implements Schedule {
  IntervalSchedule(this.period);

  final Duration period;

  @override
  bool isDue(DateTime? last, DateTime now) =>
      last == null || now.difference(last) >= period;
}

/// Fires once per day at a given wall-clock time.
class DailySchedule implements Schedule {
  DailySchedule(this.hour, {this.minute = 0});

  final int hour;
  final int minute;

  @override
  bool isDue(DateTime? last, DateTime now) {
    final todayRun = DateTime(now.year, now.month, now.day, hour, minute);
    if (now.isBefore(todayRun)) return false;
    return last == null || last.isBefore(todayRun);
  }
}

/// Fires at the minutes a 5-field cron expression matches (see
/// [Schedule.cron]). Due once per matching minute; a minute the beat didn't
/// tick in (it was down) is skipped, as cron does.
class CronSchedule implements Schedule {
  CronSchedule(this.expression) {
    final fields = expression.trim().split(RegExp(r'\s+'));
    if (fields.length != 5) {
      throw FormatException(
          'A cron expression has 5 fields (minute hour day month weekday)',
          expression);
    }
    _minutes = _field(fields[0], 0, 59);
    _hours = _field(fields[1], 0, 23);
    _days = _field(fields[2], 1, 31);
    _months = _field(fields[3], 1, 12);
    // 7 is Sunday too.
    _weekdays = _field(fields[4], 0, 7).map((d) => d % 7).toSet();
    _anyDay = fields[2] == '*';
    _anyWeekday = fields[4] == '*';
  }

  final String expression;
  late final Set<int> _minutes, _hours, _days, _months, _weekdays;
  late final bool _anyDay, _anyWeekday;

  Set<int> _field(String raw, int min, int max) {
    final out = <int>{};
    for (final part in raw.split(',')) {
      final slash = part.split('/');
      if (slash.length > 2) throw FormatException('Bad cron part', part);
      final step = slash.length == 2 ? int.tryParse(slash[1]) : 1;
      if (step == null || step < 1) throw FormatException('Bad step', part);
      int lo, hi;
      if (slash[0] == '*') {
        lo = min;
        hi = max;
      } else if (slash[0].contains('-')) {
        final ends = slash[0].split('-');
        final a = ends.length == 2 ? int.tryParse(ends[0]) : null;
        final b = ends.length == 2 ? int.tryParse(ends[1]) : null;
        if (a == null || b == null) throw FormatException('Bad range', part);
        lo = a;
        hi = b;
      } else {
        final v = int.tryParse(slash[0]);
        if (v == null) throw FormatException('Bad value', part);
        lo = v;
        hi = slash.length == 2 ? max : v;
      }
      if (lo < min || hi > max || lo > hi) {
        throw FormatException('Out of range $min-$max', part);
      }
      for (var v = lo; v <= hi; v += step) {
        out.add(v);
      }
    }
    return out;
  }

  /// Whether [t] (to the minute) is one of this schedule's times.
  bool matches(DateTime t) {
    if (!_minutes.contains(t.minute) ||
        !_hours.contains(t.hour) ||
        !_months.contains(t.month)) {
      return false;
    }
    final dayOk = _days.contains(t.day);
    final weekdayOk = _weekdays.contains(t.weekday % 7);
    if (_anyDay && _anyWeekday) return true;
    if (_anyDay) return weekdayOk;
    if (_anyWeekday) return dayOk;
    return dayOk || weekdayOk;
  }

  @override
  bool isDue(DateTime? last, DateTime now) {
    final minute = DateTime(now.year, now.month, now.day, now.hour, now.minute);
    if (!matches(minute)) return false;
    return last == null || last.isBefore(minute);
  }
}
