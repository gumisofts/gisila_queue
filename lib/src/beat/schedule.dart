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
