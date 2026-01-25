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
