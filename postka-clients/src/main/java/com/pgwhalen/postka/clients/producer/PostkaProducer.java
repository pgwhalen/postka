package com.pgwhalen.postka.clients.producer;

import com.pgwhalen.postka.common.PartitionInfo;
import com.pgwhalen.postka.common.TopicPartition;
import com.pgwhalen.postka.common.serialization.Serializer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A Postka client that publishes records to the PostgreSQL-backed message store.
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public class PostkaProducer<K, V> implements AutoCloseable {

    private final DataSource dataSource;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final ExecutorService executor;
    private volatile boolean closed = false;

    /**
     * A producer is instantiated by providing configuration properties.
     */
    public PostkaProducer(Map<String, Object> configs) {
        this(configs, null, null);
    }

    /**
     * A producer is instantiated by providing configuration properties and serializers.
     */
    public PostkaProducer(Map<String, Object> configs,
                          Serializer<K> keySerializer,
                          Serializer<V> valueSerializer) {
        this.dataSource = (DataSource) configs.get(ProducerConfig.DATASOURCE_CONFIG);
        if (this.dataSource == null) {
            throw new IllegalArgumentException("DataSource must be provided via " + ProducerConfig.DATASOURCE_CONFIG);
        }
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "postka-producer-io");
            t.setDaemon(true);
            return t;
        });

        if (this.keySerializer != null) {
            this.keySerializer.configure(configs, true);
        }
        if (this.valueSerializer != null) {
            this.valueSerializer.configure(configs, false);
        }
    }

    /**
     * Asynchronously send a record to a topic.
     *
     * @param record The record to send
     * @return A future which will contain the record metadata
     */
    public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
        return send(record, null);
    }

    /**
     * Asynchronously send a record to a topic and invoke the provided callback when the send has been acknowledged.
     *
     * @param record   The record to send
     * @param callback A user-supplied callback to execute when the record has been acknowledged
     * @return A future which will contain the record metadata
     */
    public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
        if (closed) {
            throw new IllegalStateException("Producer is closed");
        }

        return executor.submit(() -> {
            try {
                RecordMetadata metadata = doSend(record);
                if (callback != null) {
                    callback.onCompletion(metadata, null);
                }
                return metadata;
            } catch (Exception e) {
                if (callback != null) {
                    callback.onCompletion(null, e);
                }
                throw e;
            }
        });
    }

    private RecordMetadata doSend(ProducerRecord<K, V> record) throws SQLException {
        byte[] keyBytes = keySerializer != null ?
                keySerializer.serialize(record.topic(), record.key()) : null;
        byte[] valueBytes = valueSerializer != null ?
                valueSerializer.serialize(record.topic(), record.value()) : null;

        long timestamp = record.timestamp() != null ?
                record.timestamp() : System.currentTimeMillis();
        int partition = record.partition() != null ? record.partition() : 0;

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // Ensure topic exists
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT postka_ensure_topic(?, 1)")) {
                    ps.setString(1, record.topic());
                    ps.execute();
                }

                // Get next offset and insert record atomically
                long offset;
                try (PreparedStatement ps = conn.prepareStatement(
                        """
                        INSERT INTO postka_records
                            (topic_name, partition_id, offset_id, record_timestamp, key_bytes, value_bytes, headers)
                        VALUES (?, ?, postka_next_offset(?, ?), ?, ?, ?, ?::jsonb)
                        RETURNING offset_id
                        """)) {
                    ps.setString(1, record.topic());
                    ps.setInt(2, partition);
                    ps.setString(3, record.topic());
                    ps.setInt(4, partition);
                    ps.setTimestamp(5, Timestamp.from(Instant.ofEpochMilli(timestamp)));
                    ps.setBytes(6, keyBytes);
                    ps.setBytes(7, valueBytes);
                    ps.setString(8, "[]"); // TODO: serialize headers

                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        offset = rs.getLong(1);
                    }
                }

                conn.commit();

                return new RecordMetadata(
                        new TopicPartition(record.topic(), partition),
                        offset,
                        timestamp,
                        keyBytes != null ? keyBytes.length : -1,
                        valueBytes != null ? valueBytes.length : -1
                );
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        }
    }

    /**
     * Invoking this method makes all buffered records immediately available to send.
     * For Postka, this is currently a no-op since sends are synchronous.
     */
    public void flush() {
        // For hello world, sends are synchronous via executor
        // A full implementation would batch and flush pending writes
    }

    /**
     * Get the partition metadata for the given topic.
     *
     * @param topic The topic to get partition metadata for
     * @return The list of partitions
     */
    public List<PartitionInfo> partitionsFor(String topic) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT partition_id FROM postka_partitions WHERE topic_name = ? ORDER BY partition_id")) {
            ps.setString(1, topic);
            List<PartitionInfo> result = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.add(new PartitionInfo(topic, rs.getInt(1), null, null, null));
                }
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get partitions for topic: " + topic, e);
        }
    }

    /**
     * Close this producer.
     */
    @Override
    public void close() {
        close(Duration.ofMillis(Long.MAX_VALUE));
    }

    /**
     * Close this producer with a timeout.
     *
     * @param timeout The maximum time to wait for producer to complete any pending requests.
     */
    public void close(Duration timeout) {
        closed = true;
        executor.shutdown();
        try {
            executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
