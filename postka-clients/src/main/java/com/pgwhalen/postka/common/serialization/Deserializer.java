package com.pgwhalen.postka.common.serialization;

import java.util.Map;

/**
 * An interface for converting bytes to objects.
 *
 * @param <T> Type to be deserialized into.
 */
public interface Deserializer<T> extends AutoCloseable {

    /**
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     * @param isKey   whether is for key or value
     */
    default void configure(Map<String, ?> configs, boolean isKey) {
    }

    /**
     * Deserialize a record value from a byte array into a value or object.
     *
     * @param topic topic associated with the data
     * @param data  serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    T deserialize(String topic, byte[] data);

    /**
     * Close this deserializer.
     */
    @Override
    default void close() {
    }
}
