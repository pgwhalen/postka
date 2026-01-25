package com.pgwhalen.postka.clients.producer;

import com.pgwhalen.postka.common.TopicPartition;

/**
 * The metadata for a record that has been acknowledged by the server.
 */
public class RecordMetadata {
    private final TopicPartition topicPartition;
    private final long offset;
    private final long timestamp;
    private final int serializedKeySize;
    private final int serializedValueSize;

    public RecordMetadata(TopicPartition topicPartition, long offset, long timestamp,
                          int serializedKeySize, int serializedValueSize) {
        this.topicPartition = topicPartition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
    }

    /**
     * The topic the record was appended to.
     */
    public String topic() {
        return topicPartition.topic();
    }

    /**
     * The partition the record was sent to.
     */
    public int partition() {
        return topicPartition.partition();
    }

    /**
     * The offset of the record in the topic/partition.
     */
    public long offset() {
        return offset;
    }

    /**
     * The timestamp of the record in the topic/partition.
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * The size of the serialized, uncompressed key in bytes. If key is null, the returned size is -1.
     */
    public int serializedKeySize() {
        return serializedKeySize;
    }

    /**
     * The size of the serialized, uncompressed value in bytes. If value is null, the returned size is -1.
     */
    public int serializedValueSize() {
        return serializedValueSize;
    }

    @Override
    public String toString() {
        return topicPartition.toString() + "@" + offset;
    }
}
