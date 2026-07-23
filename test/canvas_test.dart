import 'package:gisila_queue/gisila_queue.dart';
import 'package:test/test.dart';

GisilaQueue _app() => GisilaQueue(
      broker: InMemoryBroker(pollInterval: const Duration(milliseconds: 5)),
      backend: InMemoryResultBackend(),
      logger: SilentQueueLogger(),
    );

void main() {
  group('chain', () {
    test('threads each result into the next step and resolves the last one',
        () async {
      final app = _app();
      final addOne =
          app.task<int>('add_one', (ctx) async => (ctx.arg(0) as int) + 1);
      final double_ =
          app.task<int>('double', (ctx) async => (ctx.arg(0) as int) * 2);
      final toStr =
          app.task<String>('to_str', (ctx) async => 'value=${ctx.arg(0)}');
      final worker = app.worker()..run();

      final result = await app.chain<String>([
        addOne.sig(args: [1]), // 1 -> 2
        double_.sig(), // 2 -> 4
        toStr.sig(), // 4 -> "value=4"
      ]).apply();

      expect(await result.get(timeout: const Duration(seconds: 2)), 'value=4');

      await worker.stop();
      await app.close();
    });

    test(
        'intermediate steps also resolve their own AsyncResult, distinct '
        'from the final chain result', () async {
      final app = _app();
      final addOne =
          app.task<int>('add_one', (ctx) async => (ctx.arg(0) as int) + 1);
      final double_ =
          app.task<int>('double', (ctx) async => (ctx.arg(0) as int) * 2);
      final worker = app.worker()..run();
      final firstStepIds = <String>[];
      worker.onSuccess = (message, result) {
        if (message.task == 'add_one') firstStepIds.add(message.id);
      };

      final finalResult = await app.chain<int>([
        addOne.sig(args: [10]), // -> 11
        double_.sig(), // -> 22
      ]).apply();

      expect(await finalResult.get(timeout: const Duration(seconds: 2)), 22);
      expect(firstStepIds, hasLength(1));
      final intermediate = app.resultFor<int>(firstStepIds.single);
      expect(await intermediate.get(timeout: const Duration(seconds: 2)), 11);

      await worker.stop();
      await app.close();
    });

    test('aborts downstream steps when a step fails, instead of hanging',
        () async {
      final app = _app();
      final ok = app.task<int>('ok', (ctx) async => 1);
      final boom =
          app.task<int>('boom', (ctx) async => throw StateError('nope'));
      final never = app.task<int>('never', (ctx) async => 999);
      final worker = app.worker()..run();

      final result = await app.chain<int>([
        ok.sig(),
        boom.sig(),
        never.sig(),
      ]).apply();

      await expectLater(
        result.get(timeout: const Duration(seconds: 2)),
        throwsA(isA<TaskFailureException>()),
      );
      expect(await result.state, TaskState.failure);

      await worker.stop();
      await app.close();
    });
  });

  group('group', () {
    test('runs every signature in parallel and collects all results', () async {
      final app = _app();
      final square = app.task<int>(
          'square', (ctx) async => (ctx.arg(0) as int) * (ctx.arg(0) as int));
      final worker = app.worker(concurrency: 4)..run();

      final results = await app.group<int>([
        square.sig(args: [2]),
        square.sig(args: [3]),
        square.sig(args: [4]),
      ]).apply();

      expect(
          await results.get(timeout: const Duration(seconds: 2)), [4, 9, 16]);

      await worker.stop();
      await app.close();
    });

    test('propagates an individual member failure through GroupResult.get',
        () async {
      final app = _app();
      final ok = app.task<int>('ok2', (ctx) async => 1);
      final boom = app.task<int>('boom2', (ctx) async => throw StateError('x'));
      final worker = app.worker()..run();

      final results = await app.group<int>([ok.sig(), boom.sig()]).apply();

      await expectLater(
        results.get(timeout: const Duration(seconds: 2)),
        throwsA(isA<TaskFailureException>()),
      );

      await worker.stop();
      await app.close();
    });
  });

  group('chord', () {
    test('fires the callback exactly once with every member result in order',
        () async {
      final app = _app();
      final square = app.task<int>('chord_square',
          (ctx) async => (ctx.arg(0) as int) * (ctx.arg(0) as int));
      var callbackRuns = 0;
      final sumUp = app.task<int>('chord_sum', (ctx) async {
        callbackRuns++;
        final results = (ctx.arg(0) as List).cast<int>();
        return results.reduce((a, b) => a + b);
      });
      final worker = app.worker(concurrency: 4)..run();

      final result = await app.chord<int>(
        [
          square.sig(args: [2]),
          square.sig(args: [3]),
          square.sig(args: [4])
        ],
        sumUp.sig(),
      ).apply();

      expect(await result.get(timeout: const Duration(seconds: 3)), 4 + 9 + 16);
      // Give any (incorrect) duplicate firing a chance to show up.
      await Future<void>.delayed(const Duration(milliseconds: 100));
      expect(callbackRuns, 1);

      await worker.stop();
      await app.close();
    });

    test('aborts the callback exactly once if any member fails', () async {
      final app = _app();
      final ok = app.task<int>('chord_ok', (ctx) async => 1);
      final boom =
          app.task<int>('chord_boom', (ctx) async => throw StateError('nope'));
      var callbackRuns = 0;
      final callback = app.task<int>('chord_never', (ctx) async {
        callbackRuns++;
        return 0;
      });
      final worker = app.worker(concurrency: 4)..run();

      final result = await app
          .chord<int>([ok.sig(), boom.sig(), ok.sig()], callback.sig()).apply();

      await expectLater(
        result.get(timeout: const Duration(seconds: 3)),
        throwsA(isA<TaskFailureException>()),
      );
      expect(await result.state, TaskState.failure);
      expect(callbackRuns, 0);

      await worker.stop();
      await app.close();
    });

    test('apply() throws without a result backend configured', () async {
      final app = GisilaQueue(
        broker: InMemoryBroker(),
        logger: SilentQueueLogger(),
      );
      final ok = app.task<int>('chord_nobackend', (ctx) async => 1);
      await expectLater(
        app.chord<int>([ok.sig()], ok.sig()).apply(),
        throwsA(isA<StateError>()),
      );
      await app.close();
    });
  });
}
