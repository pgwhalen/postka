package com.pgwhalen.postka.clients;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests against a real Kafka broker using TestContainers.
 */
@Testcontainers
public class KafkaProducerConsumerTest
        extends AbstractProducerConsumerTest<KafkaProducer<String, String>, KafkaConsumer<String, String>> {

    @Container
    static KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.0");

    private static final AtomicInteger topicCounter = new AtomicInteger();

    @BeforeAll
    static void waitForKafka() {
        // Container is started automatically by @Container annotation
    }

    @Override
    protected String uniqueTopic() {
        return "kafka-test-topic-" + topicCounter.incrementAndGet();
    }

    @Override
    protected KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    @Override
    protected KafkaConsumer<String, String> createConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(props);
    }

    @Override
    protected Future<?> send(KafkaProducer<String, String> producer, String topic, String key, String value) {
        return producer.send(new ProducerRecord<>(topic, key, value));
    }

    @Override
    protected List<TestRecord> poll(KafkaConsumer<String, String> consumer, Duration timeout) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        List<TestRecord> result = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            result.add(TestRecord.fromKafkaRecord(record));
        }
        return result;
    }

    @Override
    protected void subscribe(KafkaConsumer<String, String> consumer, Collection<String> topics) {
        consumer.subscribe(topics);
    }

    @Override
    protected void commitSync(KafkaConsumer<String, String> consumer) {
        consumer.commitSync();
    }

    @Override
    protected void closeProducer(KafkaProducer<String, String> producer) {
        producer.close();
    }

    @Override
    protected void closeConsumer(KafkaConsumer<String, String> consumer) {
        consumer.close();
    }
}
