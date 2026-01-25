package com.pgwhalen.postka.clients.producer;

import com.pgwhalen.postka.common.header.Header;
import com.pgwhalen.postka.common.header.Headers;
import com.pgwhalen.postka.common.header.RecordHeaders;

/**
 * A key/value pair to be sent to Postka. This consists of a topic name to which the record
 * is being sent, an optional partition number, and an optional key and value.
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public class ProducerRecord<K, V> {
    private final String topic;
    private final Integer partition;
    private final Long timestamp;
    private final K key;
    private final V value;
    private final Headers headers;

    /**
     * Creates a record with all fields specified.
     */
    public ProducerRecord(String topic, Integer partition, Long timestamp,
                          K key, V value, Iterable<Header> headers) {
        if (topic == null) {
            throw new IllegalArgumentException("Topic cannot be null");
        }
        if (timestamp != null && timestamp < 0) {
            throw new IllegalArgumentException("Invalid timestamp: " + timestamp);
        }
        this.topic = topic;
        this.partition = partition;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.headers = new RecordHeaders(headers);
    }

    /**
     * Creates a record with timestamp, key and value.
     */
    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value) {
        this(topic, partition, timestamp, key, value, null);
    }

    /**
     * Creates a record with partition, key and value.
     */
    public ProducerRecord(String topic, Integer partition, K key, V value) {
        this(topic, partition, null, key, value, null);
    }

    /**
     * Creates a record with key and value.
     */
    public ProducerRecord(String topic, K key, V value) {
        this(topic, null, null, key, value, null);
    }

    /**
     * Creates a record with only a value.
     */
    public ProducerRecord(String topic, V value) {
        this(topic, null, null, null, value, null);
    }

    /**
     * The topic this record is being sent to.
     */
    public String topic() {
        return topic;
    }

    /**
     * The partition to which the record will be sent (or null if no partition was specified).
     */
    public Integer partition() {
        return partition;
    }

    /**
     * The timestamp of the record.
     */
    public Long timestamp() {
        return timestamp;
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
        return "ProducerRecord(topic=" + topic +
                ", partition=" + partition +
                ", timestamp=" + timestamp +
                ", key=" + key +
                ", value=" + value +
                ", headers=" + headers + ")";
    }
}
