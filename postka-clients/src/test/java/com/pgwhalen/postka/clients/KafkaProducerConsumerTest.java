package com.pgwhalen.postka.clients;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
    protected Future<?> sendWithHeaders(KafkaProducer<String, String> producer, String topic,
                                         String key, String value, Map<String, byte[]> headers) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        for (Map.Entry<String, byte[]> header : headers.entrySet()) {
            record.headers().add(header.getKey(), header.getValue());
        }
        return producer.send(record);
    }

    @Override
    protected Future<?> sendToPartition(KafkaProducer<String, String> producer, String topic,
                                         int partition, String key, String value) {
        return producer.send(new ProducerRecord<>(topic, partition, key, value));
    }

    @Override
    protected void ensureTopicWithPartitions(String topic, int partitions) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (Admin admin = Admin.create(props)) {
            NewTopic newTopic = new NewTopic(topic, partitions, (short) 1);
            admin.createTopics(List.of(newTopic)).all().get();
        }
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

    @Override
    protected void subscribeWithListener(KafkaConsumer<String, String> consumer, Collection<String> topics,
                                          TestRebalanceListener listener) {
        consumer.subscribe(topics, new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                Set<TestTopicPartition> converted = new HashSet<>();
                for (TopicPartition tp : partitions) {
                    converted.add(new TestTopicPartition(tp.topic(), tp.partition()));
                }
                listener.onRevoked(converted);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                Set<TestTopicPartition> converted = new HashSet<>();
                for (TopicPartition tp : partitions) {
                    converted.add(new TestTopicPartition(tp.topic(), tp.partition()));
                }
                listener.onAssigned(converted);
            }
        });
    }

    @Override
    protected Set<Integer> getAssignedPartitions(KafkaConsumer<String, String> consumer, String topic) {
        Set<Integer> partitions = new HashSet<>();
        for (TopicPartition tp : consumer.assignment()) {
            if (tp.topic().equals(topic)) {
                partitions.add(tp.partition());
            }
        }
        return partitions;
    }
}
