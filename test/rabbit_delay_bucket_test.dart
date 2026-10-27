import 'package:gisila_queue/gisila_queue.dart';
import 'package:test/test.dart';

void main() {
  test('a delay goes into the largest bucket not longer than it', () {
    expect(RabbitBroker.delayBucketFor(100), 100);
    expect(RabbitBroker.delayBucketFor(199), 100);
    expect(RabbitBroker.delayBucketFor(200), 200);
    expect(RabbitBroker.delayBucketFor(1700), 1600);
    expect(RabbitBroker.delayBucketFor(60 * 60 * 1000), 3276800);
    expect(RabbitBroker.delayBucketFor(1 << 40), RabbitBroker.maxDelayBucketMs);
  });
}
