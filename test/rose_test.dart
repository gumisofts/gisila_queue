import 'dart:convert';
import 'dart:io';

import 'package:gisila_queue/gisila_queue.dart';
import 'package:gisila_queue/gisila_rose.dart';
import 'package:test/test.dart';

Future<Map<String, Object?>> _getJson(Uri uri) async {
  final client = HttpClient();
  try {
    final req = await client.getUrl(uri);
    final res = await req.close();
    final body = await res.transform(utf8.decoder).join();
    return jsonDecode(body) as Map<String, Object?>;
  } finally {
    client.close(force: true);
  }
}

Future<HttpClientResponse> _get(Uri uri) async {
  final client = HttpClient();
  final req = await client.getUrl(uri);
  return req.close();
}

void main() {
  group('GisilaRose HTTP API', () {
    late InMemoryEventBus bus;
    late InMemoryBroker broker;
    late GisilaRose rose;
    late Uri base;

    setUp(() async {
      bus = InMemoryEventBus();
      broker = InMemoryBroker();
      rose = GisilaRose(source: bus, broker: broker, port: 0);
      await rose.start();
      base = Uri.parse('http://127.0.0.1:${rose.boundPort}');
    });

    tearDown(() async {
      await rose.stop();
    });

    test('GET / serves the HTML dashboard', () async {
      final res = await _get(base);
      expect(res.statusCode, 200);
      expect(res.headers.contentType?.mimeType, 'text/html');
      final body = await res.transform(utf8.decoder).join();
      expect(body, contains('<html'));
    });

    test('GET /api/stats returns summary counters', () async {
      await bus.emit(QueueEvent(
        type: QueueEventType.taskSent,
        timestamp: DateTime.now(),
        taskId: 't1',
        taskName: 'math.add',
        queue: 'default',
      ));
      await Future<void>.delayed(const Duration(milliseconds: 50));

      final json = await _getJson(base.replace(path: '/api/stats'));
      final totals = json['totals'] as Map;
      expect(totals['sent'], greaterThanOrEqualTo(1));
    });

    test('GET /api/tasks lists tasks and GET /api/tasks/<id> fetches one',
        () async {
      await bus.emit(QueueEvent(
        type: QueueEventType.taskSent,
        timestamp: DateTime.now(),
        taskId: 'task-abc',
        taskName: 'math.add',
        queue: 'default',
        fields: {
          'args': [1, 2]
        },
      ));
      await Future<void>.delayed(const Duration(milliseconds: 50));

      final list = await _getJson(base.replace(path: '/api/tasks'));
      final tasks = list['tasks'] as List;
      expect(tasks.any((t) => (t as Map)['id'] == 'task-abc'), isTrue);

      final one = await _getJson(base.replace(path: '/api/tasks/task-abc'));
      expect(one['id'], 'task-abc');
      expect(one['name'], 'math.add');
    });

    test('GET /api/tasks/<unknown id> returns 404', () async {
      final res = await _get(base.replace(path: '/api/tasks/nope'));
      expect(res.statusCode, 404);
    });

    test('GET /api/workers returns registered workers', () async {
      await bus.emit(QueueEvent(
        type: QueueEventType.workerOnline,
        timestamp: DateTime.now(),
        workerId: 'w1',
        hostname: 'host:1',
        fields: {
          'queues': ['default'],
          'concurrency': 2,
        },
      ));
      await Future<void>.delayed(const Duration(milliseconds: 50));

      final json = await _getJson(base.replace(path: '/api/workers'));
      final workers = json['workers'] as List;
      expect(workers.any((w) => (w as Map)['id'] == 'w1'), isTrue);
    });

    test('GET /api/queues reports broker depth for known queues', () async {
      await broker.enqueue(TaskMessage(
        id: 'm1',
        task: 'math.add',
        queue: 'default',
      ));
      // The poller only refreshes queues it already knows about (from a
      // worker's announced queues, or a prior poll) — announce one via a
      // workerOnline event so 'default' is in scope.
      await bus.emit(QueueEvent(
        type: QueueEventType.workerOnline,
        timestamp: DateTime.now(),
        workerId: 'w-queues',
        fields: {
          'queues': ['default']
        },
      ));
      await Future<void>.delayed(const Duration(milliseconds: 50));
      await rose.pollQueuesNow();

      final json = await _getJson(base.replace(path: '/api/queues'));
      final queues = json['queues'] as Map;
      expect(queues['default'], 1);
    });

    test('unknown route returns 404 JSON', () async {
      final res = await _get(base.replace(path: '/nope'));
      expect(res.statusCode, 404);
      final body = await res.transform(utf8.decoder).join();
      expect(jsonDecode(body), {'error': 'not found'});
    });
  });
}
