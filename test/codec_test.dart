import 'package:gisila_queue/gisila_queue.dart';
import 'package:test/test.dart';

GisilaQueue _app() => GisilaQueue(
      broker: InMemoryBroker(pollInterval: const Duration(milliseconds: 5)),
      backend: InMemoryResultBackend(),
      logger: SilentQueueLogger(),
    );

void main() {
  group('MessageCodec', () {
    test('JsonMessageCodec round-trips a message envelope', () {
      const codec = JsonMessageCodec();
      final raw = codec.encode({
        'a': 1,
        'b': ['x', 'y']
      });
      expect(codec.decode(raw), {
        'a': 1,
        'b': ['x', 'y']
      });
    });

    test('TaskMessage.encode/decode use the default codec transparently', () {
      final msg = TaskMessage(
        id: 'abc',
        task: 'math.add',
        queue: 'default',
        args: [1, 2],
      );
      final back = TaskMessage.decode(msg.encode());
      expect(back.id, 'abc');
      expect(back.args, [1, 2]);
    });
  });

  group('validateEncodable', () {
    test('passes through plain JSON-compatible args/kwargs', () {
      expect(
        () => validateEncodable('t', [
          1,
          'two',
          3.0,
          null,
          true
        ], {
          'k': [1, 2]
        }),
        returnsNormally,
      );
    });

    test('throws UnencodableArgumentsException for a non-JSON value', () {
      expect(
        () => validateEncodable('t', [DateTime.now()], const {}),
        throwsA(isA<UnencodableArgumentsException>()),
      );
    });
  });

  group('GisilaQueue.send eager validation', () {
    test('rejects unencodable args at the call site, before enqueuing',
        () async {
      final app = _app();
      final t = app.task<void>('sink', (ctx) async {
        return null;
      });

      await expectLater(
        t.delay([DateTime.now()]),
        throwsA(isA<UnencodableArgumentsException>()),
      );
      // Nothing should have reached the broker.
      expect(await app.broker.length('default'), 0);
    });

    test('rejects unencodable kwargs the same way', () async {
      final app = _app();
      final t = app.task<void>('sink2', (ctx) async {
        return null;
      });

      await expectLater(
        t.applyAsync(kwargs: {'when': DateTime.now()}),
        throwsA(isA<UnencodableArgumentsException>()),
      );
    });

    test('plain JSON-compatible args still enqueue normally', () async {
      final app = _app();
      final t = app.task<int>('sink3', (ctx) async => (ctx.arg(0) as int) + 1);
      final worker = app.worker()..run();

      final r = await t.delay([41]);
      expect(await r.get(timeout: const Duration(seconds: 2)), 42);

      await worker.stop();
    });
  });
}
