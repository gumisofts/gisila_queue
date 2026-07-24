import 'dart:convert';
import 'dart:io';

import 'package:gisila_queue/gisila_queue.dart';
import 'package:gisila_queue/gisila_rose.dart';
import 'package:test/test.dart';

Future<Map<String, Object?>> _getJson(
  Uri uri, {
  String? basicAuth,
}) async {
  final client = HttpClient();
  try {
    final req = await client.getUrl(uri);
    if (basicAuth != null) {
      req.headers.set(
        HttpHeaders.authorizationHeader,
        'Basic ${base64Encode(utf8.encode(basicAuth))}',
      );
    }
    final res = await req.close();
    final body = await res.transform(utf8.decoder).join();
    return jsonDecode(body) as Map<String, Object?>;
  } finally {
    client.close(force: true);
  }
}

Future<HttpClientResponse> _get(Uri uri, {String? basicAuth}) async {
  final client = HttpClient();
  final req = await client.getUrl(uri);
  if (basicAuth != null) {
    req.headers.set(
      HttpHeaders.authorizationHeader,
      'Basic ${base64Encode(utf8.encode(basicAuth))}',
    );
  }
  return req.close();
}

Future<({int status, Map<String, Object?>? json})> _postJson(
  Uri uri, {
  Object? body,
  String? basicAuth,
}) async {
  final client = HttpClient();
  try {
    final req = await client.postUrl(uri);
    if (basicAuth != null) {
      req.headers.set(
        HttpHeaders.authorizationHeader,
        'Basic ${base64Encode(utf8.encode(basicAuth))}',
      );
    }
    req.headers.contentType = ContentType.json;
    if (body != null) req.write(jsonEncode(body));
    final res = await req.close();
    final text = await res.transform(utf8.decoder).join();
    Map<String, Object?>? json;
    if (text.isNotEmpty) {
      json = jsonDecode(text) as Map<String, Object?>;
    }
    return (status: res.statusCode, json: json);
  } finally {
    client.close(force: true);
  }
}

void main() {
  group('GisilaRose HTTP API', () {
    late InMemoryEventBus bus;
    late InMemoryBroker broker;
    late InMemoryControlChannel control;
    late InMemoryResultBackend backend;
    late GisilaRose rose;
    late Uri base;

    setUp(() async {
      bus = InMemoryEventBus();
      broker = InMemoryBroker();
      control = InMemoryControlChannel();
      backend = InMemoryResultBackend();
      rose = GisilaRose(
        source: bus,
        broker: broker,
        control: control,
        events: bus,
        backend: backend,
        port: 0,
      );
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
      expect(body, contains('GisilaRose'));
    });

    test('GET /api/stats returns summary counters and capabilities', () async {
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
      final caps = json['capabilities'] as Map;
      expect(caps['control'], isTrue);
      expect(caps['fire'], isTrue);
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

    test('GET /api/tasks filters by worker', () async {
      final now = DateTime.now();
      await bus.emit(QueueEvent(
        type: QueueEventType.taskStarted,
        timestamp: now,
        taskId: 'on-w1',
        taskName: 'a',
        queue: 'default',
        workerId: 'w1',
      ));
      await bus.emit(QueueEvent(
        type: QueueEventType.taskStarted,
        timestamp: now,
        taskId: 'on-w2',
        taskName: 'b',
        queue: 'default',
        workerId: 'w2',
      ));
      await Future<void>.delayed(const Duration(milliseconds: 50));

      final json = await _getJson(
        base.replace(path: '/api/tasks', queryParameters: {'worker': 'w1'}),
      );
      final tasks = json['tasks'] as List;
      expect(tasks, hasLength(1));
      expect((tasks.first as Map)['id'], 'on-w1');
    });

    test('GET /api/tasks/<unknown id> returns 404', () async {
      final res = await _get(base.replace(path: '/api/tasks/nope'));
      expect(res.statusCode, 404);
    });

    test('GET /api/workers includes active_task_ids', () async {
      final now = DateTime.now();
      await bus.emit(QueueEvent(
        type: QueueEventType.workerOnline,
        timestamp: now,
        workerId: 'w1',
        hostname: 'host:1',
        fields: {
          'queues': ['default'],
          'concurrency': 2,
        },
      ));
      await bus.emit(QueueEvent(
        type: QueueEventType.taskStarted,
        timestamp: now,
        taskId: 'running-1',
        taskName: 'work.do',
        queue: 'default',
        workerId: 'w1',
      ));
      await Future<void>.delayed(const Duration(milliseconds: 50));

      final json = await _getJson(base.replace(path: '/api/workers'));
      final workers = json['workers'] as List;
      final w1 = workers.cast<Map>().firstWhere((w) => w['id'] == 'w1');
      expect(w1['active_task_ids'], contains('running-1'));

      final detail = await _getJson(base.replace(path: '/api/workers/w1'));
      expect((detail['worker'] as Map)['id'], 'w1');
      expect(detail['active_tasks'], isNotEmpty);
    });

    test('GET /api/queues reports broker depth for known queues', () async {
      await broker.enqueue(TaskMessage(
        id: 'm1',
        task: 'math.add',
        queue: 'default',
      ));
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

    test('POST /api/tasks/fire enqueues a new task', () async {
      final res = await _postJson(
        base.replace(path: '/api/tasks/fire'),
        body: {
          'name': 'health.ping',
          'args': <Object?>[],
          'kwargs': <String, Object?>{},
          'queue': 'default',
        },
      );
      expect(res.status, 200);
      final id = res.json!['id'] as String;
      expect(id, isNotEmpty);
      expect(await broker.length('default'), greaterThanOrEqualTo(1));
      final stored = await backend.fetch(id);
      expect(stored?.state, TaskState.pending);

      await Future<void>.delayed(const Duration(milliseconds: 50));
      final listed = await _getJson(base.replace(path: '/api/tasks'));
      final tasks = listed['tasks'] as List;
      expect(tasks.any((t) => (t as Map)['id'] == id), isTrue);
    });

    test('POST /api/tasks/<id>/retry re-fires with a new id', () async {
      final t0 = DateTime.now().subtract(const Duration(seconds: 1));
      await bus.emit(QueueEvent(
        type: QueueEventType.taskSent,
        timestamp: t0,
        taskId: 'failed-1',
        taskName: 'failure.test',
        queue: 'jobs',
        fields: {
          'args': ['x'],
          'kwargs': {'k': 1},
        },
      ));
      await bus.emit(QueueEvent(
        type: QueueEventType.taskFailed,
        timestamp: DateTime.now(),
        taskId: 'failed-1',
        taskName: 'failure.test',
        queue: 'jobs',
        fields: {'error': 'boom'},
      ));
      await Future<void>.delayed(const Duration(milliseconds: 50));

      final res = await _postJson(
        base.replace(path: '/api/tasks/failed-1/retry'),
      );
      expect(res.status, 200);
      final newId = res.json!['id'] as String;
      expect(newId, isNot('failed-1'));
      expect(res.json!['retried_from'], 'failed-1');
      expect(await broker.length('jobs'), 1);
    });

    test('POST /api/tasks/<id>/revoke publishes control message', () async {
      final msgs = <ControlMessage>[];
      final sub = control.subscribe().listen(msgs.add);
      addTearDown(sub.cancel);

      final res = await _postJson(
        base.replace(path: '/api/tasks/some-id/revoke'),
      );
      expect(res.status, 200);
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(msgs, hasLength(1));
      expect(msgs.first.type, ControlMessageType.revoke);
      expect(msgs.first.taskId, 'some-id');
    });

    test('POST /api/queues/<name>/pause and resume', () async {
      final msgs = <ControlMessage>[];
      final sub = control.subscribe().listen(msgs.add);
      addTearDown(sub.cancel);

      var res = await _postJson(base.replace(path: '/api/queues/default/pause'));
      expect(res.status, 200);
      res = await _postJson(base.replace(path: '/api/queues/default/resume'));
      expect(res.status, 200);
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(msgs.map((m) => m.type), [
        ControlMessageType.pause,
        ControlMessageType.resume,
      ]);
    });

    test('unknown route returns 404 JSON', () async {
      final res = await _get(base.replace(path: '/nope'));
      expect(res.statusCode, 404);
      final body = await res.transform(utf8.decoder).join();
      expect(jsonDecode(body), {'error': 'not found'});
    });
  });

  group('GisilaRose basic auth', () {
    late GisilaRose rose;
    late Uri base;

    setUp(() async {
      final bus = InMemoryEventBus();
      rose = GisilaRose(
        source: bus,
        broker: InMemoryBroker(),
        control: InMemoryControlChannel(),
        events: bus,
        basicAuth: 'admin:s3cret',
        port: 0,
      );
      await rose.start();
      base = Uri.parse('http://127.0.0.1:${rose.boundPort}');
    });

    tearDown(() async {
      await rose.stop();
    });

    test('rejects missing credentials with 401', () async {
      final res = await _get(base);
      expect(res.statusCode, 401);
      expect(res.headers.value(HttpHeaders.wwwAuthenticateHeader),
          contains('Basic'));
    });

    test('accepts valid basic auth', () async {
      final res = await _get(base, basicAuth: 'admin:s3cret');
      expect(res.statusCode, 200);
      final stats = await _getJson(
        base.replace(path: '/api/stats'),
        basicAuth: 'admin:s3cret',
      );
      expect(stats['capabilities'], isNotNull);
    });

    test('rejects wrong password', () async {
      final res = await _get(base, basicAuth: 'admin:wrong');
      expect(res.statusCode, 401);
    });
  });

  group('GisilaRose without control/broker', () {
    late GisilaRose rose;
    late Uri base;
    late InMemoryEventBus bus;

    setUp(() async {
      bus = InMemoryEventBus();
      rose = GisilaRose(source: bus, port: 0);
      await rose.start();
      base = Uri.parse('http://127.0.0.1:${rose.boundPort}');
    });

    tearDown(() async {
      await rose.stop();
    });

    test('mutating endpoints return 503', () async {
      final fire = await _postJson(
        base.replace(path: '/api/tasks/fire'),
        body: {'name': 'x'},
      );
      expect(fire.status, 503);
      expect(fire.json!['error'], contains('broker'));

      final revoke = await _postJson(
        base.replace(path: '/api/tasks/x/revoke'),
      );
      expect(revoke.status, 503);

      final pause = await _postJson(
        base.replace(path: '/api/queues/default/pause'),
      );
      expect(pause.status, 503);
    });
  });
}
