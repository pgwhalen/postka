package com.pgwhalen.postka.clients.consumer;

import com.pgwhalen.postka.common.TopicPartition;
import com.pgwhalen.postka.common.header.RecordHeaders;
import com.pgwhalen.postka.common.serialization.Deserializer;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A Postka client that consumes records from the PostgreSQL-backed message store.
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public class PostkaConsumer<K, V> implements AutoCloseable {

    private final DataSource dataSource;
    private final String groupId;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    private final Set<String> subscribedTopics = ConcurrentHashMap.newKeySet();
    private final Map<TopicPartition, Long> positions = new ConcurrentHashMap<>();
    private volatile boolean closed = false;

    /**
     * A consumer is instantiated by providing configuration properties.
     */
    public PostkaConsumer(Map<String, Object> configs) {
        this(configs, null, null);
    }

    /**
     * A consumer is instantiated by providing configuration properties and deserializers.
     */
    public PostkaConsumer(Map<String, Object> configs,
                          Deserializer<K> keyDeserializer,
                          Deserializer<V> valueDeserializer) {
        this.dataSource = (DataSource) configs.get(ConsumerConfig.DATASOURCE_CONFIG);
        if (this.dataSource == null) {
            throw new IllegalArgumentException("DataSource must be provided via " + ConsumerConfig.DATASOURCE_CONFIG);
        }
        this.groupId = (String) configs.get(ConsumerConfig.GROUP_ID_CONFIG);
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;

        if (this.keyDeserializer != null) {
            this.keyDeserializer.configure(configs, true);
        }
        if (this.valueDeserializer != null) {
            this.valueDeserializer.configure(configs, false);
        }
    }

    /**
     * Subscribe to the given list of topics to get dynamically assigned partitions.
     *
     * @param topics The list of topics to subscribe to
     */
    public void subscribe(Collection<String> topics) {
        subscribedTopics.clear();
        subscribedTopics.addAll(topics);
        positions.clear();
        // Load committed offsets for subscribed topics
        loadCommittedOffsets();
    }

    /**
     * Get the current subscription.
     */
    public Set<String> subscription() {
        return Set.copyOf(subscribedTopics);
    }

    /**
     * Unsubscribe from topics currently subscribed with subscribe.
     */
    public void unsubscribe() {
        subscribedTopics.clear();
        positions.clear();
    }

    private void loadCommittedOffsets() {
        if (groupId == null) return;

        try (Connection conn = dataSource.getConnection()) {
            for (String topic : subscribedTopics) {
                try (PreparedStatement ps = conn.prepareStatement(
                        """
                        SELECT partition_id, committed_offset
                        FROM postka_consumer_offsets
                        WHERE group_id = ? AND topic_name = ?
                        """)) {
                    ps.setString(1, groupId);
                    ps.setString(2, topic);
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            TopicPartition tp = new TopicPartition(topic, rs.getInt(1));
                            positions.put(tp, rs.getLong(2) + 1); // Start after committed
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to load committed offsets", e);
        }
    }

    /**
     * Fetch data for the topics or partitions specified using subscribe.
     *
     * @param timeout The time to block waiting for data
     * @return A ConsumerRecords object with the fetched records
     */
    public ConsumerRecords<K, V> poll(Duration timeout) {
        if (closed) {
            throw new IllegalStateException("Consumer is closed");
        }
        if (subscribedTopics.isEmpty()) {
            return ConsumerRecords.empty();
        }

        Map<TopicPartition, List<ConsumerRecord<K, V>>> result = new HashMap<>();

        try (Connection conn = dataSource.getConnection()) {
            for (String topic : subscribedTopics) {
                // Get partitions for this topic
                List<Integer> partitions = getPartitions(conn, topic);

                for (int partition : partitions) {
                    TopicPartition tp = new TopicPartition(topic, partition);
                    long startOffset = positions.getOrDefault(tp, 0L);

                    List<ConsumerRecord<K, V>> records = fetchRecords(conn, tp, startOffset);
                    if (!records.isEmpty()) {
                        result.put(tp, records);
                        // Update position to after last record
                        long lastOffset = records.get(records.size() - 1).offset();
                        positions.put(tp, lastOffset + 1);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to poll records", e);
        }

        return new ConsumerRecords<>(result);
    }

    private List<Integer> getPartitions(Connection conn, String topic) throws SQLException {
        List<Integer> partitions = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT partition_id FROM postka_partitions WHERE topic_name = ? ORDER BY partition_id")) {
            ps.setString(1, topic);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    partitions.add(rs.getInt(1));
                }
            }
        }
        return partitions;
    }

    private List<ConsumerRecord<K, V>> fetchRecords(Connection conn, TopicPartition tp,
                                                    long startOffset) throws SQLException {
        List<ConsumerRecord<K, V>> records = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                """
                SELECT offset_id, timestamp_ms, key_bytes, value_bytes, headers
                FROM postka_records
                WHERE topic_name = ? AND partition_id = ? AND offset_id >= ?
                ORDER BY offset_id
                LIMIT 500
                """)) {
            ps.setString(1, tp.topic());
            ps.setInt(2, tp.partition());
            ps.setLong(3, startOffset);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long offset = rs.getLong("offset_id");
                    long timestamp = rs.getLong("timestamp_ms");
                    byte[] keyBytes = rs.getBytes("key_bytes");
                    byte[] valueBytes = rs.getBytes("value_bytes");

                    K key = keyDeserializer != null ?
                            keyDeserializer.deserialize(tp.topic(), keyBytes) : null;
                    V value = valueDeserializer != null ?
                            valueDeserializer.deserialize(tp.topic(), valueBytes) : null;

                    records.add(new ConsumerRecord<>(
                            tp.topic(), tp.partition(), offset, timestamp,
                            keyBytes != null ? keyBytes.length : -1,
                            valueBytes != null ? valueBytes.length : -1,
                            key, value, new RecordHeaders()
                    ));
                }
            }
        }
        return records;
    }

    /**
     * Commit offsets returned on the last poll for all the subscribed partitions.
     */
    public void commitSync() {
        commitSync(Duration.ofSeconds(30));
    }

    /**
     * Commit offsets returned on the last poll for all the subscribed partitions.
     *
     * @param timeout The maximum amount of time to await completion
     */
    public void commitSync(Duration timeout) {
        if (groupId == null) {
            throw new IllegalStateException("Cannot commit offsets without a group.id");
        }

        try (Connection conn = dataSource.getConnection()) {
            for (Map.Entry<TopicPartition, Long> entry : positions.entrySet()) {
                TopicPartition tp = entry.getKey();
                long position = entry.getValue();
                if (position > 0) {
                    commitOffset(conn, tp, position - 1); // Commit last consumed
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to commit offsets", e);
        }
    }

    private void commitOffset(Connection conn, TopicPartition tp, long offset) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                """
                INSERT INTO postka_consumer_offsets (group_id, topic_name, partition_id, committed_offset)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (group_id, topic_name, partition_id)
                DO UPDATE SET committed_offset = EXCLUDED.committed_offset,
                              committed_at = CURRENT_TIMESTAMP
                """)) {
            ps.setString(1, groupId);
            ps.setString(2, tp.topic());
            ps.setInt(3, tp.partition());
            ps.setLong(4, offset);
            ps.executeUpdate();
        }
    }

    /**
     * Get the offset of the next record that will be fetched.
     *
     * @param partition The partition to get the position for
     * @return The offset
     */
    public long position(TopicPartition partition) {
        return positions.getOrDefault(partition, 0L);
    }

    /**
     * Overrides the fetch offsets that the consumer will use on the next poll.
     *
     * @param partition The partition to seek
     * @param offset    The offset to seek to
     */
    public void seek(TopicPartition partition, long offset) {
        positions.put(partition, offset);
    }

    /**
     * Close the consumer.
     */
    @Override
    public void close() {
        close(Duration.ofMillis(Long.MAX_VALUE));
    }

    /**
     * Close the consumer with a timeout.
     *
     * @param timeout The maximum time to wait for the consumer to close
     */
    public void close(Duration timeout) {
        closed = true;
    }
}
