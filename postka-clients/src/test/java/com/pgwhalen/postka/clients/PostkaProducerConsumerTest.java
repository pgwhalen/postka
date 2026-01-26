package com.pgwhalen.postka.clients;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.pgwhalen.postka.clients.consumer.ConsumerConfig;
import com.pgwhalen.postka.clients.consumer.ConsumerRecord;
import com.pgwhalen.postka.clients.consumer.ConsumerRecords;
import com.pgwhalen.postka.clients.consumer.PostkaConsumer;
import com.pgwhalen.postka.clients.producer.PostkaProducer;
import com.pgwhalen.postka.clients.producer.ProducerConfig;
import com.pgwhalen.postka.clients.producer.ProducerRecord;
import com.pgwhalen.postka.common.header.RecordHeader;
import com.pgwhalen.postka.common.serialization.StringDeserializer;
import com.pgwhalen.postka.common.serialization.StringSerializer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests against a real PostgreSQL database using TestContainers.
 */
@Testcontainers
public class PostkaProducerConsumerTest
        extends AbstractProducerConsumerTest<PostkaProducer<String, String>, PostkaConsumer<String, String>> {

    /** @noinspection resource*/
    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("postka_test")
            .withUsername("postka")
            .withPassword("postka");

    private static DataSource dataSource;
    private static final AtomicInteger topicCounter = new AtomicInteger();

    @BeforeAll
    static void setupDatabase() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        config.setMaximumPoolSize(10);
        dataSource = new HikariDataSource(config);

        // Run Flyway migrations
        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load();
        flyway.migrate();
    }

    @Override
    protected String uniqueTopic() {
        return "postka-test-topic-" + topicCounter.incrementAndGet();
    }

    @Override
    protected PostkaProducer<String, String> createProducer() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.DATASOURCE_CONFIG, dataSource);
        return new PostkaProducer<>(configs, new StringSerializer(), new StringSerializer());
    }

    @Override
    protected PostkaConsumer<String, String> createConsumer(String groupId) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.DATASOURCE_CONFIG, dataSource);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return new PostkaConsumer<>(configs, new StringDeserializer(), new StringDeserializer());
    }

    @Override
    protected Future<?> send(PostkaProducer<String, String> producer, String topic, String key, String value) {
        return producer.send(new ProducerRecord<>(topic, key, value));
    }

    @Override
    protected Future<?> sendWithHeaders(PostkaProducer<String, String> producer, String topic,
                                         String key, String value, Map<String, byte[]> headers) {
        List<com.pgwhalen.postka.common.header.Header> headerList = new ArrayList<>();
        for (Map.Entry<String, byte[]> header : headers.entrySet()) {
            headerList.add(new RecordHeader(header.getKey(), header.getValue()));
        }
        return producer.send(new ProducerRecord<>(topic, null, null, key, value, headerList));
    }

    @Override
    protected Future<?> sendToPartition(PostkaProducer<String, String> producer, String topic,
                                         int partition, String key, String value) {
        return producer.send(new ProducerRecord<>(topic, partition, key, value));
    }

    @Override
    protected void ensureTopicWithPartitions(String topic, int partitions) throws Exception {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement("SELECT postka_ensure_topic(?, ?)")) {
            ps.setString(1, topic);
            ps.setInt(2, partitions);
            ps.execute();
        }
    }

    @Override
    protected List<TestRecord> poll(PostkaConsumer<String, String> consumer, Duration timeout) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        List<TestRecord> result = new ArrayList<>();
        for (ConsumerRecord<String, String> record : records) {
            result.add(TestRecord.fromPostkaRecord(record));
        }
        return result;
    }

    @Override
    protected void subscribe(PostkaConsumer<String, String> consumer, Collection<String> topics) {
        consumer.subscribe(topics);
    }

    @Override
    protected void commitSync(PostkaConsumer<String, String> consumer) {
        consumer.commitSync();
    }

    @Override
    protected void closeProducer(PostkaProducer<String, String> producer) {
        producer.close();
    }

    @Override
    protected void closeConsumer(PostkaConsumer<String, String> consumer) {
        consumer.close();
    }
}
