package com.pgwhalen.postka.clients;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Abstract test defining the contract that both Kafka and Postka must satisfy.
 * Subclasses provide the actual producer/consumer instances.
 *
 * @param <P> The producer type
 * @param <C> The consumer type
 */
public abstract class AbstractProducerConsumerTest<P, C> {

    protected abstract P createProducer();

    protected abstract C createConsumer(String groupId);

    protected abstract Future<?> send(P producer, String topic, String key, String value);

    protected abstract List<TestRecord> poll(C consumer, Duration timeout);

    protected abstract void subscribe(C consumer, Collection<String> topics);

    protected abstract void commitSync(C consumer);

    protected abstract void closeProducer(P producer);

    protected abstract void closeConsumer(C consumer);

    protected abstract String uniqueTopic();

    protected abstract Future<?> sendWithHeaders(P producer, String topic, String key, String value,
                                                  Map<String, byte[]> headers);

    protected abstract Future<?> sendToPartition(P producer, String topic, int partition, String key, String value);

    protected abstract void ensureTopicWithPartitions(String topic, int partitions) throws Exception;

    protected record TestRecord(String topic, int partition, long offset, String key, String value,
                                Map<String, byte[]> headers) {
        static TestRecord fromKafkaRecord(ConsumerRecord<String, String> record) {
            Map<String, byte[]> headers = new java.util.HashMap<>();
            for (org.apache.kafka.common.header.Header header : record.headers()) {
                headers.put(header.key(), header.value());
            }
            return new TestRecord(record.topic(), record.partition(),
                    record.offset(), record.key(), record.value(), headers);
        }

        static TestRecord fromPostkaRecord(com.pgwhalen.postka.clients.consumer.ConsumerRecord<String, String> record) {
            Map<String, byte[]> headers = new java.util.HashMap<>();
            for (com.pgwhalen.postka.common.header.Header header : record.headers()) {
                headers.put(header.key(), header.value());
            }
            return new TestRecord(record.topic(), record.partition(),
                    record.offset(), record.key(), record.value(), headers);
        }
    }

    @Test
    void testSendAndReceive() throws Exception {
        String topic = uniqueTopic();
        P producer = createProducer();
        C consumer = createConsumer("test-group-" + System.currentTimeMillis());

        try {
            // Send a message
            Future<?> future = send(producer, topic, "key1", "value1");
            future.get(10, TimeUnit.SECONDS);

            // Subscribe and poll
            subscribe(consumer, List.of(topic));

            List<TestRecord> records = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 10_000;
            while (records.isEmpty() && System.currentTimeMillis() < deadline) {
                records.addAll(poll(consumer, Duration.ofMillis(500)));
            }

            // Verify
            assertFalse(records.isEmpty(), "Should have received at least one record");
            TestRecord record = records.getFirst();
            assertEquals(topic, record.topic());
            assertEquals("key1", record.key());
            assertEquals("value1", record.value());
            assertEquals(0, record.partition());
            assertEquals(0, record.offset());
        } finally {
            closeProducer(producer);
            closeConsumer(consumer);
        }
    }

    @Test
    void testMultipleMessages() throws Exception {
        String topic = uniqueTopic();
        P producer = createProducer();
        C consumer = createConsumer("test-group-multi-" + System.currentTimeMillis());

        try {
            // Send multiple messages
            for (int i = 0; i < 5; i++) {
                send(producer, topic, "key" + i, "value" + i).get(5, TimeUnit.SECONDS);
            }

            // Subscribe and poll all
            subscribe(consumer, List.of(topic));

            List<TestRecord> records = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 15_000;
            while (records.size() < 5 && System.currentTimeMillis() < deadline) {
                records.addAll(poll(consumer, Duration.ofMillis(500)));
            }

            // Verify order and offsets
            assertEquals(5, records.size(), "Should have received 5 records");
            for (int i = 0; i < 5; i++) {
                assertEquals(i, records.get(i).offset(), "Offset should match");
                assertEquals("key" + i, records.get(i).key(), "Key should match");
                assertEquals("value" + i, records.get(i).value(), "Value should match");
            }
        } finally {
            closeProducer(producer);
            closeConsumer(consumer);
        }
    }

    @Test
    void testCommitOffset() throws Exception {
        String topic = uniqueTopic();
        String groupId = "test-group-commit-" + System.currentTimeMillis();
        P producer = createProducer();

        try {
            // Send messages
            for (int i = 0; i < 3; i++) {
                send(producer, topic, "key" + i, "value" + i).get(5, TimeUnit.SECONDS);
            }

            // First consumer reads and commits
            C consumer1 = createConsumer(groupId);
            subscribe(consumer1, List.of(topic));
            List<TestRecord> records1 = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 10_000;
            while (records1.size() < 3 && System.currentTimeMillis() < deadline) {
                records1.addAll(poll(consumer1, Duration.ofMillis(500)));
            }
            assertEquals(3, records1.size(), "First consumer should receive 3 records");
            commitSync(consumer1);
            closeConsumer(consumer1);

            // Second consumer with same group should get no new messages
            C consumer2 = createConsumer(groupId);
            subscribe(consumer2, List.of(topic));
            List<TestRecord> records2 = poll(consumer2, Duration.ofSeconds(2));
            assertTrue(records2.isEmpty(), "Should not receive already-committed messages");
            closeConsumer(consumer2);
        } finally {
            closeProducer(producer);
        }
    }

    @Test
    void testMultiplePartitions() throws Exception {
        String topic = uniqueTopic();
        int numPartitions = 3;
        ensureTopicWithPartitions(topic, numPartitions);

        P producer = createProducer();
        C consumer = createConsumer("test-group-partitions-" + System.currentTimeMillis());

        try {
            // Send messages to different partitions
            sendToPartition(producer, topic, 0, "key-p0-a", "value-p0-a").get(5, TimeUnit.SECONDS);
            sendToPartition(producer, topic, 0, "key-p0-b", "value-p0-b").get(5, TimeUnit.SECONDS);
            sendToPartition(producer, topic, 1, "key-p1-a", "value-p1-a").get(5, TimeUnit.SECONDS);
            sendToPartition(producer, topic, 2, "key-p2-a", "value-p2-a").get(5, TimeUnit.SECONDS);
            sendToPartition(producer, topic, 2, "key-p2-b", "value-p2-b").get(5, TimeUnit.SECONDS);

            // Subscribe and poll
            subscribe(consumer, List.of(topic));

            List<TestRecord> allRecords = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 15_000;
            while (allRecords.size() < 5 && System.currentTimeMillis() < deadline) {
                allRecords.addAll(poll(consumer, Duration.ofMillis(500)));
            }

            // Verify we got all 5 records
            assertEquals(5, allRecords.size(), "Should have received 5 records total");

            // Group by partition and verify
            List<TestRecord> partition0Records = allRecords.stream()
                    .filter(r -> r.partition() == 0)
                    .sorted((a, b) -> Long.compare(a.offset(), b.offset()))
                    .toList();
            List<TestRecord> partition1Records = allRecords.stream()
                    .filter(r -> r.partition() == 1)
                    .sorted((a, b) -> Long.compare(a.offset(), b.offset()))
                    .toList();
            List<TestRecord> partition2Records = allRecords.stream()
                    .filter(r -> r.partition() == 2)
                    .sorted((a, b) -> Long.compare(a.offset(), b.offset()))
                    .toList();

            // Verify partition 0 has 2 messages with independent offsets starting at 0
            assertEquals(2, partition0Records.size(), "Partition 0 should have 2 records");
            assertEquals(0, partition0Records.get(0).offset(), "First message in partition 0 should have offset 0");
            assertEquals(1, partition0Records.get(1).offset(), "Second message in partition 0 should have offset 1");
            assertEquals("key-p0-a", partition0Records.get(0).key());
            assertEquals("key-p0-b", partition0Records.get(1).key());

            // Verify partition 1 has 1 message with offset 0
            assertEquals(1, partition1Records.size(), "Partition 1 should have 1 record");
            assertEquals(0, partition1Records.getFirst().offset(), "First message in partition 1 should have offset 0");
            assertEquals("key-p1-a", partition1Records.getFirst().key());

            // Verify partition 2 has 2 messages with independent offsets starting at 0
            assertEquals(2, partition2Records.size(), "Partition 2 should have 2 records");
            assertEquals(0, partition2Records.get(0).offset(), "First message in partition 2 should have offset 0");
            assertEquals(1, partition2Records.get(1).offset(), "Second message in partition 2 should have offset 1");
            assertEquals("key-p2-a", partition2Records.get(0).key());
            assertEquals("key-p2-b", partition2Records.get(1).key());
        } finally {
            closeProducer(producer);
            closeConsumer(consumer);
        }
    }

    @Test
    void testMultipleTopics() throws Exception {
        String topic1 = uniqueTopic();
        String topic2 = uniqueTopic();
        String topic3 = uniqueTopic();
        P producer = createProducer();
        C consumer = createConsumer("test-group-multi-topic-" + System.currentTimeMillis());

        try {
            // Send messages to different topics
            send(producer, topic1, "key1-a", "value1-a").get(5, TimeUnit.SECONDS);
            send(producer, topic1, "key1-b", "value1-b").get(5, TimeUnit.SECONDS);
            send(producer, topic2, "key2-a", "value2-a").get(5, TimeUnit.SECONDS);
            send(producer, topic3, "key3-a", "value3-a").get(5, TimeUnit.SECONDS);
            send(producer, topic3, "key3-b", "value3-b").get(5, TimeUnit.SECONDS);
            send(producer, topic3, "key3-c", "value3-c").get(5, TimeUnit.SECONDS);

            // Subscribe to all topics
            subscribe(consumer, List.of(topic1, topic2, topic3));

            List<TestRecord> allRecords = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 15_000;
            while (allRecords.size() < 6 && System.currentTimeMillis() < deadline) {
                allRecords.addAll(poll(consumer, Duration.ofMillis(500)));
            }

            // Verify we got all 6 records
            assertEquals(6, allRecords.size(), "Should have received 6 records total");

            // Group by topic and verify
            List<TestRecord> topic1Records = allRecords.stream()
                    .filter(r -> r.topic().equals(topic1))
                    .toList();
            List<TestRecord> topic2Records = allRecords.stream()
                    .filter(r -> r.topic().equals(topic2))
                    .toList();
            List<TestRecord> topic3Records = allRecords.stream()
                    .filter(r -> r.topic().equals(topic3))
                    .toList();

            // Verify topic1 has 2 messages with correct offsets
            assertEquals(2, topic1Records.size(), "Topic1 should have 2 records");
            assertTrue(topic1Records.stream().anyMatch(r -> r.offset() == 0 && r.key().equals("key1-a")));
            assertTrue(topic1Records.stream().anyMatch(r -> r.offset() == 1 && r.key().equals("key1-b")));

            // Verify topic2 has 1 message
            assertEquals(1, topic2Records.size(), "Topic2 should have 1 record");
            assertEquals(0, topic2Records.getFirst().offset(), "Topic2 first message should have offset 0");
            assertEquals("key2-a", topic2Records.getFirst().key());

            // Verify topic3 has 3 messages with correct offsets
            assertEquals(3, topic3Records.size(), "Topic3 should have 3 records");
            assertTrue(topic3Records.stream().anyMatch(r -> r.offset() == 0 && r.key().equals("key3-a")));
            assertTrue(topic3Records.stream().anyMatch(r -> r.offset() == 1 && r.key().equals("key3-b")));
            assertTrue(topic3Records.stream().anyMatch(r -> r.offset() == 2 && r.key().equals("key3-c")));
        } finally {
            closeProducer(producer);
            closeConsumer(consumer);
        }
    }

    @Test
    void testPollTimeoutReturnsImmediatelyWithZeroTimeout() throws Exception {
        String topic = uniqueTopic();
        C consumer = createConsumer("test-group-timeout-zero-" + System.currentTimeMillis());

        try {
            subscribe(consumer, List.of(topic));

            // With zero timeout, poll should return immediately even with no records
            long start = System.currentTimeMillis();
            List<TestRecord> records = poll(consumer, Duration.ZERO);
            long elapsed = System.currentTimeMillis() - start;

            assertTrue(records.isEmpty(), "Should have no records");
            assertTrue(elapsed < 500, "Zero timeout poll should return quickly, took " + elapsed + "ms");
        } finally {
            closeConsumer(consumer);
        }
    }

    @Test
    void testPollTimeoutBlocksWhenNoRecords() throws Exception {
        String topic = uniqueTopic();
        C consumer = createConsumer("test-group-timeout-blocks-" + System.currentTimeMillis());

        try {
            subscribe(consumer, List.of(topic));

            // Poll should block for approximately the timeout duration when no records exist
            long timeoutMs = 1000;
            long start = System.currentTimeMillis();
            List<TestRecord> records = poll(consumer, Duration.ofMillis(timeoutMs));
            long elapsed = System.currentTimeMillis() - start;

            assertTrue(records.isEmpty(), "Should have no records");
            // Should have blocked for at least most of the timeout (allow 200ms tolerance)
            assertTrue(elapsed >= timeoutMs - 200,
                    "Poll should have blocked for at least " + (timeoutMs - 200) + "ms, but only took " + elapsed + "ms");
            // Should not have blocked for much longer than the timeout (allow 500ms tolerance for slow CI)
            assertTrue(elapsed < timeoutMs + 500,
                    "Poll should not block much longer than timeout, took " + elapsed + "ms");
        } finally {
            closeConsumer(consumer);
        }
    }

    @Test
    void testPollReturnsImmediatelyWhenRecordsAvailable() throws Exception {
        String topic = uniqueTopic();
        P producer = createProducer();
        C consumer = createConsumer("test-group-timeout-immediate-" + System.currentTimeMillis());

        try {
            // Send a message first
            send(producer, topic, "key1", "value1").get(10, TimeUnit.SECONDS);

            // Subscribe and poll with a long timeout
            subscribe(consumer, List.of(topic));

            // First poll may need to wait for partition assignment, so we warm up
            List<TestRecord> warmup = new ArrayList<>();
            long warmupDeadline = System.currentTimeMillis() + 10_000;
            while (warmup.isEmpty() && System.currentTimeMillis() < warmupDeadline) {
                warmup.addAll(poll(consumer, Duration.ofMillis(500)));
            }
            assertFalse(warmup.isEmpty(), "Should have received the warmup record");

            // Now send another message
            send(producer, topic, "key2", "value2").get(10, TimeUnit.SECONDS);

            // Poll with a long timeout - should return quickly since records are available
            long longTimeoutMs = 10_000;
            long start = System.currentTimeMillis();
            List<TestRecord> records = poll(consumer, Duration.ofMillis(longTimeoutMs));
            long elapsed = System.currentTimeMillis() - start;

            assertFalse(records.isEmpty(), "Should have received records");
            // Should return quickly, not wait for the full timeout
            assertTrue(elapsed < 2000,
                    "Poll with available records should return quickly, but took " + elapsed + "ms");
        } finally {
            closeProducer(producer);
            closeConsumer(consumer);
        }
    }

    @Test
    void testRecordHeaders() throws Exception {
        String topic = uniqueTopic();
        P producer = createProducer();
        C consumer = createConsumer("test-group-headers-" + System.currentTimeMillis());

        try {
            // Send a message with headers
            Map<String, byte[]> sentHeaders = Map.of(
                    "header1", "value1".getBytes(StandardCharsets.UTF_8),
                    "header2", new byte[]{1, 2, 3, 4, 5}
            );
            Future<?> future = sendWithHeaders(producer, topic, "key1", "value1", sentHeaders);
            future.get(10, TimeUnit.SECONDS);

            // Subscribe and poll
            subscribe(consumer, List.of(topic));

            List<TestRecord> records = new ArrayList<>();
            long deadline = System.currentTimeMillis() + 10_000;
            while (records.isEmpty() && System.currentTimeMillis() < deadline) {
                records.addAll(poll(consumer, Duration.ofMillis(500)));
            }

            // Verify record was received
            assertFalse(records.isEmpty(), "Should have received at least one record");
            TestRecord record = records.getFirst();
            assertEquals(topic, record.topic());
            assertEquals("key1", record.key());
            assertEquals("value1", record.value());

            // Verify headers
            assertNotNull(record.headers(), "Headers should not be null");
            assertEquals(2, record.headers().size(), "Should have 2 headers");
            assertArrayEquals("value1".getBytes(StandardCharsets.UTF_8),
                    record.headers().get("header1"), "Header1 value should match");
            assertArrayEquals(new byte[]{1, 2, 3, 4, 5},
                    record.headers().get("header2"), "Header2 value should match");
        } finally {
            closeProducer(producer);
            closeConsumer(consumer);
        }
    }
}
