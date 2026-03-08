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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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
    private Connection connection;
    private PreparedStatement commitOffsetStatement;

    // Consumer group coordination fields
    private final String memberId = UUID.randomUUID().toString();
    private final int heartbeatIntervalMs;
    private final int sessionTimeoutMs;
    private final Map<String, Integer> currentGenerationPerTopic = new ConcurrentHashMap<>();
    private final Set<TopicPartition> assignedPartitions = ConcurrentHashMap.newKeySet();
    private ConsumerRebalanceListener rebalanceListener = NoOpRebalanceListener.INSTANCE;
    private ScheduledExecutorService heartbeatExecutor;
    private ScheduledFuture<?> heartbeatFuture;

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

        // Read heartbeat and session timeout configs
        Object heartbeatConfig = configs.get(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG);
        this.heartbeatIntervalMs = heartbeatConfig != null
                ? Integer.parseInt(heartbeatConfig.toString())
                : ConsumerConfig.DEFAULT_HEARTBEAT_INTERVAL_MS;

        Object sessionConfig = configs.get(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG);
        this.sessionTimeoutMs = sessionConfig != null
                ? Integer.parseInt(sessionConfig.toString())
                : ConsumerConfig.DEFAULT_SESSION_TIMEOUT_MS;

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
        subscribe(topics, NoOpRebalanceListener.INSTANCE);
    }

    /**
     * Subscribe to the given list of topics to get dynamically assigned partitions.
     *
     * @param topics   The list of topics to subscribe to
     * @param listener Non-null listener instance to get notifications on partition assignment/revocation
     */
    public void subscribe(Collection<String> topics, ConsumerRebalanceListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        }
        this.rebalanceListener = listener;
        subscribedTopics.clear();
        subscribedTopics.addAll(topics);
        positions.clear();
        assignedPartitions.clear();
        currentGenerationPerTopic.clear();

        // Acquire connection for polling
        try {
            closeConnection();
            connection = dataSource.getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to acquire connection", e);
        }

        // If we have a group ID, join the consumer group and get partition assignments
        if (groupId != null) {
            joinConsumerGroup();
            startHeartbeat();
        }

        // Load committed offsets for assigned partitions
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
        stopHeartbeat();
        leaveConsumerGroup();
        subscribedTopics.clear();
        positions.clear();
        assignedPartitions.clear();
        currentGenerationPerTopic.clear();
        rebalanceListener = NoOpRebalanceListener.INSTANCE;
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
                            // Only load offsets for partitions we're assigned to
                            if (assignedPartitions.isEmpty() || assignedPartitions.contains(tp)) {
                                positions.put(tp, rs.getLong(2) + 1); // Start after committed
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to load committed offsets", e);
        }
    }

    /**
     * Join the consumer group and get initial partition assignments.
     */
    private void joinConsumerGroup() {
        if (groupId == null || connection == null) return;

        try {
            for (String topic : subscribedTopics) {
                JoinResult result = callJoinGroup(topic);
                handleRebalance(topic, result.assignedPartitions, result.generationId);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to join consumer group", e);
        }
    }

    private JoinResult callJoinGroup(String topic) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(
                "SELECT * FROM postka_join_group(?, ?, ?, ?)")) {
            ps.setString(1, groupId);
            ps.setString(2, memberId);
            ps.setString(3, topic);
            ps.setInt(4, sessionTimeoutMs);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int generationId = rs.getInt(1);
                    boolean rebalanceTriggered = rs.getBoolean(2);
                    Array partitionsArray = rs.getArray(3);
                    Set<Integer> partitions = new HashSet<>();
                    if (partitionsArray != null) {
                        Integer[] arr = (Integer[]) partitionsArray.getArray();
                        for (Integer p : arr) {
                            partitions.add(p);
                        }
                    }
                    return new JoinResult(generationId, rebalanceTriggered, partitions);
                }
                return new JoinResult(0, false, Set.of());
            }
        }
    }

    private record JoinResult(int generationId, boolean rebalanceTriggered, Set<Integer> assignedPartitions) {}

    /**
     * Handle a rebalance event for a specific topic.
     */
    private void handleRebalance(String topic, Set<Integer> newPartitions, int newGeneration) {
        // Skip if no generation change for this topic and we already have assignments for it
        Integer currentGen = currentGenerationPerTopic.get(topic);
        boolean hasAssignmentsForTopic = assignedPartitions.stream()
                .anyMatch(tp -> tp.topic().equals(topic));
        if (currentGen != null && currentGen == newGeneration && hasAssignmentsForTopic) {
            return;
        }

        // Compute the old partitions for this topic
        Set<TopicPartition> oldPartitionsForTopic = new HashSet<>();
        for (TopicPartition tp : assignedPartitions) {
            if (tp.topic().equals(topic)) {
                oldPartitionsForTopic.add(tp);
            }
        }

        // Convert new partition IDs to TopicPartition objects
        Set<TopicPartition> newPartitionsSet = new HashSet<>();
        for (Integer partition : newPartitions) {
            newPartitionsSet.add(new TopicPartition(topic, partition));
        }

        // Compute revoked and assigned partitions
        Set<TopicPartition> revokedPartitions = new HashSet<>(oldPartitionsForTopic);
        revokedPartitions.removeAll(newPartitionsSet);

        Set<TopicPartition> newlyAssignedPartitions = new HashSet<>(newPartitionsSet);
        newlyAssignedPartitions.removeAll(oldPartitionsForTopic);

        // Call onPartitionsRevoked for revoked partitions
        if (!revokedPartitions.isEmpty()) {
            rebalanceListener.onPartitionsRevoked(revokedPartitions);
            // Remove positions for revoked partitions
            for (TopicPartition tp : revokedPartitions) {
                positions.remove(tp);
            }
        }

        // Update assigned partitions
        assignedPartitions.removeAll(oldPartitionsForTopic);
        assignedPartitions.addAll(newPartitionsSet);

        // Call onPartitionsAssigned for newly assigned partitions
        if (!newlyAssignedPartitions.isEmpty()) {
            rebalanceListener.onPartitionsAssigned(newlyAssignedPartitions);
            // Load committed offsets for newly assigned partitions
            loadCommittedOffsetsForPartitions(newlyAssignedPartitions);
        }

        // Update generation for this topic
        currentGenerationPerTopic.put(topic, newGeneration);
    }

    private void loadCommittedOffsetsForPartitions(Set<TopicPartition> partitions) {
        if (groupId == null || connection == null || partitions.isEmpty()) return;

        try {
            for (TopicPartition tp : partitions) {
                try (PreparedStatement ps = connection.prepareStatement(
                        """
                        SELECT committed_offset
                        FROM postka_consumer_offsets
                        WHERE group_id = ? AND topic_name = ? AND partition_id = ?
                        """)) {
                    ps.setString(1, groupId);
                    ps.setString(2, tp.topic());
                    ps.setInt(3, tp.partition());
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            positions.put(tp, rs.getLong(1) + 1); // Start after committed
                        } else {
                            positions.put(tp, 0L); // No committed offset, start from beginning
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to load committed offsets", e);
        }
    }

    /**
     * Start the heartbeat executor.
     */
    private void startHeartbeat() {
        if (heartbeatExecutor == null) {
            heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "postka-heartbeat-" + memberId.substring(0, 8));
                t.setDaemon(true);
                return t;
            });
        }
        heartbeatFuture = heartbeatExecutor.scheduleAtFixedRate(
                this::sendHeartbeat,
                heartbeatIntervalMs,
                heartbeatIntervalMs,
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Stop the heartbeat executor.
     */
    private void stopHeartbeat() {
        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(false);
            heartbeatFuture = null;
        }
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdownNow();
            heartbeatExecutor = null;
        }
    }

    /**
     * Send a heartbeat to the coordinator and check for rebalance.
     */
    private void sendHeartbeat() {
        if (closed || connection == null || groupId == null) return;

        try {
            for (String topic : subscribedTopics) {
                JoinResult result = callJoinGroup(topic);
                // If generation changed, we need to handle rebalance
                Integer currentGen = currentGenerationPerTopic.get(topic);
                if (currentGen == null || result.generationId != currentGen) {
                    handleRebalance(topic, result.assignedPartitions, result.generationId);
                }
            }
        } catch (SQLException e) {
            // Log error but don't throw - heartbeat failures are recoverable
            // In production, we might want to trigger a rejoin
        }
    }

    /**
     * Check for rebalance at the start of poll.
     */
    private void checkForRebalance() {
        if (groupId == null || connection == null) return;

        try {
            for (String topic : subscribedTopics) {
                // Query current assignment without triggering join
                try (PreparedStatement ps = connection.prepareStatement(
                        "SELECT * FROM postka_get_assignment(?, ?, ?)")) {
                    ps.setString(1, groupId);
                    ps.setString(2, memberId);
                    ps.setString(3, topic);

                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            int generationId = rs.getInt(1);
                            Array partitionsArray = rs.getArray(3);
                            Set<Integer> partitions = new HashSet<>();
                            if (partitionsArray != null) {
                                Integer[] arr = (Integer[]) partitionsArray.getArray();
                                for (Integer p : arr) {
                                    partitions.add(p);
                                }
                            }
                            Integer currentGen = currentGenerationPerTopic.get(topic);
                            if (currentGen == null || generationId != currentGen) {
                                handleRebalance(topic, partitions, generationId);
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to check for rebalance", e);
        }
    }

    /**
     * Leave the consumer group.
     */
    private void leaveConsumerGroup() {
        if (groupId == null || connection == null) return;

        try {
            for (String topic : subscribedTopics) {
                try (PreparedStatement ps = connection.prepareStatement(
                        "SELECT postka_leave_group(?, ?, ?)")) {
                    ps.setString(1, groupId);
                    ps.setString(2, memberId);
                    ps.setString(3, topic);
                    ps.execute();
                }
            }
        } catch (SQLException e) {
            // Ignore errors during leave - we're closing anyway
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

        // Check for rebalance at start of poll
        checkForRebalance();

        long timeoutMillis = timeout.toMillis();
        long deadline = System.currentTimeMillis() + timeoutMillis;
        long pollIntervalMillis = 100; // Poll the database every 100ms

        while (!closed) {
            Map<TopicPartition, List<ConsumerRecord<K, V>>> result = new HashMap<>();

            try {
                for (String topic : subscribedTopics) {
                    // Build partition offset map from current positions for this topic
                    // Only include partitions we're assigned to (when using consumer groups)
                    Map<Integer, Long> partitionOffsets = new HashMap<>();
                    for (Map.Entry<TopicPartition, Long> entry : positions.entrySet()) {
                        if (entry.getKey().topic().equals(topic)) {
                            // If we have assigned partitions, only fetch from those
                            if (assignedPartitions.isEmpty() || assignedPartitions.contains(entry.getKey())) {
                                partitionOffsets.put(entry.getKey().partition(), entry.getValue());
                            }
                        }
                    }

                    // If using consumer groups and no partitions assigned, skip this topic
                    if (groupId != null && !assignedPartitions.isEmpty()) {
                        // Build partition offsets only for assigned partitions
                        partitionOffsets.clear();
                        for (TopicPartition tp : assignedPartitions) {
                            if (tp.topic().equals(topic)) {
                                partitionOffsets.put(tp.partition(), positions.getOrDefault(tp, 0L));
                            }
                        }
                    }

                    // Fetch records using the table function
                    Map<TopicPartition, List<ConsumerRecord<K, V>>> topicRecords =
                            fetchRecordsForTopic(connection, topic, partitionOffsets);

                    // Update positions and collect results
                    for (Map.Entry<TopicPartition, List<ConsumerRecord<K, V>>> entry : topicRecords.entrySet()) {
                        TopicPartition tp = entry.getKey();
                        List<ConsumerRecord<K, V>> records = entry.getValue();
                        if (!records.isEmpty()) {
                            // Only include records from assigned partitions when using consumer groups
                            if (assignedPartitions.isEmpty() || assignedPartitions.contains(tp)) {
                                result.put(tp, records);
                                long lastOffset = records.getLast().offset();
                                positions.put(tp, lastOffset + 1);
                            }
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

    private Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchRecordsForTopic(
            Connection conn, String topic, Map<Integer, Long> partitionOffsets) throws SQLException {
        Map<TopicPartition, List<ConsumerRecord<K, V>>> result = new HashMap<>();

        // Build array of (partition_id, start_offset) tuples for the function
        String partitionOffsetsArray = buildPartitionOffsetsArray(partitionOffsets);

        String sql = """
                SELECT partition_id, offset_id, record_timestamp, key_bytes, value_bytes, headers
                FROM postka_fetch_records(?, ?::postka_partition_offset[])
                """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, topic);
            ps.setString(2, partitionOffsetsArray);

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

    private String buildPartitionOffsetsArray(Map<Integer, Long> partitionOffsets) {
        if (partitionOffsets == null || partitionOffsets.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<Integer, Long> entry : partitionOffsets.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            sb.append("\"(").append(entry.getKey()).append(",").append(entry.getValue()).append(")\"");
            first = false;
        }
        sb.append("}");
        return sb.toString();
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
     * Get the set of partitions currently assigned to this consumer.
     *
     * @return The set of partitions currently assigned to this consumer
     */
    public Set<TopicPartition> assignment() {
        return Set.copyOf(assignedPartitions);
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
        stopHeartbeat();
        leaveConsumerGroup();
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
