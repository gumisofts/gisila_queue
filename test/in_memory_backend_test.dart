import 'package:gisila_queue/gisila_queue.dart';
import 'package:test/test.dart';

StoredResult _result({DateTime? updatedAt}) => StoredResult(
      state: TaskState.success,
      result: 'ok',
      updatedAt: updatedAt ?? DateTime.now(),
    );

void main() {
  group('InMemoryResultBackend', () {
    test('is unbounded by default, matching prior behaviour', () async {
      final backend = InMemoryResultBackend();
      for (var i = 0; i < 50; i++) {
        await backend.store('task-$i', _result());
      }
      for (var i = 0; i < 50; i++) {
        expect(await backend.fetch('task-$i'), isNotNull);
      }
    });

    test('evicts the oldest-touched entry once maxEntries is exceeded',
        () async {
      final backend = InMemoryResultBackend(maxEntries: 3);
      await backend.store('a', _result());
      await backend.store('b', _result());
      await backend.store('c', _result());
      await backend.store('d', _result()); // evicts 'a'

      expect(await backend.fetch('a'), isNull);
      expect(await backend.fetch('b'), isNotNull);
      expect(await backend.fetch('c'), isNotNull);
      expect(await backend.fetch('d'), isNotNull);
    });

    test(
        're-storing an existing id refreshes its recency instead of '
        'evicting it', () async {
      final backend = InMemoryResultBackend(maxEntries: 2);
      await backend.store('a', _result());
      await backend.store('b', _result());
      await backend.store('a', _result()); // touch 'a' again -> now newest
      await backend.store('c', _result()); // should evict 'b', not 'a'

      expect(await backend.fetch('a'), isNotNull);
      expect(await backend.fetch('b'), isNull);
      expect(await backend.fetch('c'), isNotNull);
    });

    test('fetch treats an entry older than ttl as absent', () async {
      final backend = InMemoryResultBackend(ttl: const Duration(minutes: 1));
      await backend.store(
        'stale',
        _result(updatedAt: DateTime.now().subtract(const Duration(minutes: 5))),
      );
      await backend.store('fresh', _result());

      expect(await backend.fetch('stale'), isNull);
      expect(await backend.fetch('fresh'), isNotNull);
    });

    test('forget removes an entry regardless of ttl/maxEntries', () async {
      final backend = InMemoryResultBackend(maxEntries: 10);
      await backend.store('a', _result());
      await backend.forget('a');
      expect(await backend.fetch('a'), isNull);
    });
  });
}
