package com.pgwhalen.postka.clients.consumer;

import com.pgwhalen.postka.common.TopicPartition;
import com.pgwhalen.postka.common.header.Header;
import com.pgwhalen.postka.common.header.RecordHeader;
import com.pgwhalen.postka.common.header.RecordHeaders;
import com.pgwhalen.postka.common.serialization.Deserializer;

import javax.sql.DataSource;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
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
    private final Map<String, String> topicTableNames = new ConcurrentHashMap<>();
    private volatile boolean closed = false;
    private Connection connection;
    private PreparedStatement commitOffsetStatement;

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
        topicTableNames.clear();
        // Acquire connection for polling
        try {
            closeConnection();
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to acquire connection", e);
        }
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
        topicTableNames.clear();
        closeConnection();
    }

    private void loadCommittedOffsets() {
        if (groupId == null || connection == null) return;

        try {
            for (String topic : subscribedTopics) {
                try (PreparedStatement ps = connection.prepareStatement(
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
     * This method blocks for at most the given timeout waiting for records.
     *
     * @param timeout The maximum time to block waiting for data. If zero or negative,
     *                returns immediately with any available records (or empty if none).
     * @return A ConsumerRecords object with the fetched records
     */
    public ConsumerRecords<K, V> poll(Duration timeout) {
        if (closed) {
            throw new IllegalStateException("Consumer is closed");
        }
        if (subscribedTopics.isEmpty() || connection == null) {
            return ConsumerRecords.empty();
        }

        long timeoutMillis = timeout.toMillis();
        long deadline = System.currentTimeMillis() + timeoutMillis;
        long pollIntervalMillis = 100; // Poll the database every 100ms

        while (!closed) {
            Map<TopicPartition, List<ConsumerRecord<K, V>>> result = new HashMap<>();

            try {
                for (String topic : subscribedTopics) {
                    // Get partitions and build offset map for this topic
                    List<Integer> partitions = getPartitions(connection, topic);
                    Map<Integer, Long> partitionOffsets = new HashMap<>();
                    for (int partition : partitions) {
                        TopicPartition tp = new TopicPartition(topic, partition);
                        partitionOffsets.put(partition, positions.getOrDefault(tp, 0L));
                    }

                    // Fetch all partitions in a single query
                    Map<TopicPartition, List<ConsumerRecord<K, V>>> topicRecords =
                            fetchRecordsForTopic(connection, topic, partitionOffsets);

                    // Update positions and collect results
                    for (Map.Entry<TopicPartition, List<ConsumerRecord<K, V>>> entry : topicRecords.entrySet()) {
                        TopicPartition tp = entry.getKey();
                        List<ConsumerRecord<K, V>> records = entry.getValue();
                        if (!records.isEmpty()) {
                            result.put(tp, records);
                            long lastOffset = records.getLast().offset();
                            positions.put(tp, lastOffset + 1);
                        }
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException("Failed to poll records", e);
            }

            // If we found records, return immediately
            if (!result.isEmpty()) {
                return new ConsumerRecords<>(result);
            }

            // If timeout is zero or negative, or we've exceeded the deadline, return empty
            long remaining = deadline - System.currentTimeMillis();
            if (timeoutMillis <= 0 || remaining <= 0) {
                return ConsumerRecords.empty();
            }

            // Wait before polling again, but not longer than remaining time
            try {
                Thread.sleep(Math.min(pollIntervalMillis, remaining));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return ConsumerRecords.empty();
            }
        }

        // Consumer was closed during poll
        throw new IllegalStateException("Consumer is closed");
    }

    private List<Integer> getPartitions(Connection conn, String topic) throws SQLException {
        List<Integer> partitions = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT partition_count FROM postka_topics WHERE topic_name = ?")) {
            ps.setString(1, topic);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int partitionCount = rs.getInt(1);
                    for (int i = 0; i < partitionCount; i++) {
                        partitions.add(i);
                    }
                }
            }
        }
        return partitions;
    }

    private String getRecordsTableName(Connection conn, String topic) throws SQLException {
        String tableName = topicTableNames.get(topic);
        if (tableName != null) {
            return tableName;
        }

        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT postka_get_records_table(?)")) {
            ps.setString(1, topic);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    tableName = rs.getString(1);
                    if (tableName != null) {
                        topicTableNames.put(topic, tableName);
                    }
                }
            }
        }
        return tableName;
    }

    private Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchRecordsForTopic(
            Connection conn, String topic, Map<Integer, Long> partitionOffsets) throws SQLException {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> result = new HashMap<>();

        String tableName = getRecordsTableName(conn, topic);
        if (tableName == null || partitionOffsets.isEmpty()) {
            return result;
        }

        // Build OR clauses for each partition with its starting offset
        StringBuilder whereClause = new StringBuilder();
        List<Object> params = new ArrayList<>();
        boolean first = true;
        for (Map.Entry<Integer, Long> entry : partitionOffsets.entrySet()) {
            if (!first) {
                whereClause.append(" OR ");
            }
            whereClause.append("(partition_id = ? AND offset_id >= ?)");
            params.add(entry.getKey());
            params.add(entry.getValue());
            first = false;
        }

        String sql = String.format(
                """
                SELECT partition_id, offset_id, record_timestamp, key_bytes, value_bytes, headers
                FROM %s
                WHERE %s
                ORDER BY partition_id, offset_id
                """, tableName, whereClause);

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < params.size(); i++) {
                Object param = params.get(i);
                if (param instanceof Integer) {
                    ps.setInt(i + 1, (Integer) param);
                } else {
                    ps.setLong(i + 1, (Long) param);
                }
            }

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int partition = rs.getInt("partition_id");
                    long offset = rs.getLong("offset_id");
                    Timestamp ts = rs.getTimestamp("record_timestamp");
                    long timestamp = ts != null ? ts.getTime() : 0L;
                    byte[] keyBytes = rs.getBytes("key_bytes");
                    byte[] valueBytes = rs.getBytes("value_bytes");
                    RecordHeaders headers = parseHeaders(rs.getArray("headers"));

                    K key = keyDeserializer != null ?
                            keyDeserializer.deserialize(topic, keyBytes) : null;
                    V value = valueDeserializer != null ?
                            valueDeserializer.deserialize(topic, valueBytes) : null;

                    TopicPartition tp = new TopicPartition(topic, partition);
                    result.computeIfAbsent(tp, k -> new ArrayList<>()).add(new ConsumerRecord<>(
                            topic, partition, offset, timestamp,
                            keyBytes != null ? keyBytes.length : -1,
                            valueBytes != null ? valueBytes.length : -1,
                            key, value, headers
                    ));
                }
            }
        }
        return result;
    }

    private RecordHeaders parseHeaders(Array headersArray) throws SQLException {
        List<Header> headers = new ArrayList<>();
        if (headersArray != null) {
            Object[] array = (Object[]) headersArray.getArray();
            for (Object element : array) {
                if (element instanceof org.postgresql.util.PGobject pgObject) {
                    String value = pgObject.getValue();
                    if (value != null && value.startsWith("(") && value.endsWith(")")) {
                        String[] parts = parseCompositeValue(value.substring(1, value.length() - 1));
                        if (parts.length == 2) {
                            String key = unescapeCompositeField(parts[0]);
                            byte[] valueBytes = parseByteaHex(parts[1]);
                            headers.add(new RecordHeader(key, valueBytes));
                        }
                    }
                }
            }
        }
        return new RecordHeaders(headers);
    }

    private String[] parseCompositeValue(String value) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        boolean escaped = false;

        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (escaped) {
                current.append(c);
                escaped = false;
            } else if (c == '\\') {
                current.append(c);
                escaped = true;
            } else if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == ',' && !inQuotes) {
                parts.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        parts.add(current.toString());
        return parts.toArray(new String[0]);
    }

    private String unescapeCompositeField(String field) {
        if (field == null || field.isEmpty()) {
            return field;
        }
        if (field.startsWith("\"") && field.endsWith("\"")) {
            field = field.substring(1, field.length() - 1);
        }
        return field.replace("\\\"", "\"").replace("\\\\", "\\");
    }

    private byte[] parseByteaHex(String hex) {
        if (hex == null || hex.isEmpty()) {
            return null;
        }
        String hexPart = hex;
        if (hexPart.startsWith("\"") && hexPart.endsWith("\"")) {
            hexPart = hexPart.substring(1, hexPart.length() - 1);
        }
        if (hexPart.startsWith("\\\\x")) {
            hexPart = hexPart.substring(3);
        } else if (hexPart.startsWith("\\x")) {
            hexPart = hexPart.substring(2);
        } else {
            return null;
        }
        byte[] result = new byte[hexPart.length() / 2];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) Integer.parseInt(hexPart.substring(i * 2, i * 2 + 2), 16);
        }
        return result;
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
        if (connection == null) {
            throw new IllegalStateException("Consumer is not subscribed");
        }

        try {
            for (Map.Entry<TopicPartition, Long> entry : positions.entrySet()) {
                TopicPartition tp = entry.getKey();
                long position = entry.getValue();
                if (position > 0) {
                    commitOffset(tp, position - 1); // Commit last consumed
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to commit offsets", e);
        }
    }

    private void commitOffset(TopicPartition tp, long offset) throws SQLException {
        if (commitOffsetStatement == null) {
            commitOffsetStatement = connection.prepareStatement(
                    """
                    INSERT INTO postka_consumer_offsets (group_id, topic_name, partition_id, committed_offset)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT (group_id, topic_name, partition_id)
                    DO UPDATE SET committed_offset = EXCLUDED.committed_offset,
                                  committed_at = (NOW() AT TIME ZONE 'UTC')
                    """);
        }
        commitOffsetStatement.setString(1, groupId);
        commitOffsetStatement.setString(2, tp.topic());
        commitOffsetStatement.setInt(3, tp.partition());
        commitOffsetStatement.setLong(4, offset);
        commitOffsetStatement.executeUpdate();
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
        closeConnection();
    }

    private void closeConnection() {
        if (commitOffsetStatement != null) {
            try {
                commitOffsetStatement.close();
            } catch (SQLException e) {
                // Ignore close errors
            }
            commitOffsetStatement = null;
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // Ignore close errors
            }
            connection = null;
        }
    }
}
