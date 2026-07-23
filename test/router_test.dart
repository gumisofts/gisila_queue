import 'package:gisila_queue/gisila_queue.dart';
import 'package:test/test.dart';

GisilaQueue _app({Router? router}) => GisilaQueue(
      broker: InMemoryBroker(pollInterval: const Duration(milliseconds: 5)),
      backend: InMemoryResultBackend(),
      logger: SilentQueueLogger(),
      defaultQueue: 'default',
      router: router,
    );

void main() {
  group('Router', () {
    test('resolve matches glob patterns and returns null otherwise', () {
      final router = Router()
        ..route('emails.*', queue: 'emails')
        ..route('reports.daily', queue: 'reports');

      expect(router.resolve('emails.send_welcome'), 'emails');
      expect(router.resolve('reports.daily'), 'reports');
      expect(router.resolve('reports.weekly'), isNull);
    });

    test('first matching rule wins', () {
      final router = Router()
        ..route('emails.*', queue: 'emails')
        ..route('emails.urgent', queue: 'urgent-emails');

      expect(router.resolve('emails.urgent'), 'emails');
    });

    test('routeMatching accepts an arbitrary RegExp', () {
      final router = Router()
        ..routeMatching(RegExp(r'^batch_\d+$'), queue: 'batch');
      expect(router.resolve('batch_42'), 'batch');
      expect(router.resolve('batch_x'), isNull);
    });
  });

  group('GisilaQueue queue resolution precedence', () {
    test('an explicit queue always wins', () async {
      final router = Router()..route('*', queue: 'from-router');
      final app = _app(router: router);
      app.task<void>('t', (ctx) async => null, queue: 'task-default');
      final r = await app.send<void>('t', queue: 'explicit');
      expect((await r.state), TaskState.pending);
      await app.close();
    });

    test(
        'router wins over the task default queue when no explicit queue is given',
        () async {
      final router = Router()..route('t2', queue: 'from-router');
      final app = _app(router: router);
      final ref = app.task<int>('t2', (ctx) async => 1, queue: 'task-default');
      final worker = app.worker(queues: ['from-router'])..run();

      final r = await ref.delay();
      expect(await r.get(timeout: const Duration(seconds: 2)), 1);

      await worker.stop();
      await app.close();
    });

    test('falls back to the task default queue when no router matches',
        () async {
      final app = _app(router: Router());
      final ref = app.task<int>('t3', (ctx) async => 2, queue: 'task-default');
      final worker = app.worker(queues: ['task-default'])..run();

      final r = await ref.delay();
      expect(await r.get(timeout: const Duration(seconds: 2)), 2);

      await worker.stop();
      await app.close();
    });

    test('falls back to the app default queue when nothing else applies',
        () async {
      final app = _app();
      app.task<int>('t4', (ctx) async => 3);
      final worker = app.worker(queues: ['default'])..run();

      final r = await app.send<int>('t4');
      expect(await r.get(timeout: const Duration(seconds: 2)), 3);

      await worker.stop();
      await app.close();
    });
  });
}
