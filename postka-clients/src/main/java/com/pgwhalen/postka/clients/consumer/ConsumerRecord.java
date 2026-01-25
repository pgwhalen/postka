package com.pgwhalen.postka.clients.consumer;

import com.pgwhalen.postka.common.header.Headers;

/**
 * A key/value pair to be received from Postka. This also consists of a topic name and
 * a partition number from which the record is being received, an offset that points
 * to the record in a partition, and a timestamp.
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public class ConsumerRecord<K, V> {
    public static final int NULL_SIZE = -1;

    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final int serializedKeySize;
    private final int serializedValueSize;
    private final K key;
    private final V value;
    private final Headers headers;

    public ConsumerRecord(String topic, int partition, long offset, long timestamp,
                          int serializedKeySize, int serializedValueSize,
                          K key, V value, Headers headers) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.key = key;
        this.value = value;
        this.headers = headers;
    }

    /**
     * The topic this record is received from.
     */
    public String topic() {
        return topic;
    }

    /**
     * The partition from which this record is received.
     */
    public int partition() {
        return partition;
    }

    /**
     * The position of this record in the corresponding partition.
     */
    public long offset() {
        return offset;
    }

    /**
     * The timestamp of this record.
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

    /**
     * The key (or null if no key is specified).
     */
    public K key() {
        return key;
    }

    /**
     * The value.
     */
    public V value() {
        return value;
    }

    /**
     * The headers.
     */
    public Headers headers() {
        return headers;
    }

    @Override
    public String toString() {
        return "ConsumerRecord(topic = " + topic +
                ", partition = " + partition +
                ", offset = " + offset +
                ", timestamp = " + timestamp +
                ", key = " + key +
                ", value = " + value + ")";
    }
}
