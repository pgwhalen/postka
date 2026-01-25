package com.pgwhalen.postka.common.serialization;

import java.util.Map;

/**
 * An interface for converting objects to bytes.
 *
 * @param <T> Type to be serialized from.
 */
public interface Serializer<T> extends AutoCloseable {

    /**
     * Configure this class.
     *
     * @param configs configs in key/value pairs
     * @param isKey   whether is for key or value
     */
    default void configure(Map<String, ?> configs, boolean isKey) {
    }

    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic topic associated with data
     * @param data  typed data
     * @return serialized bytes
     */
    byte[] serialize(String topic, T data);

    /**
     * Close this serializer.
     */
    @Override
    default void close() {
    }
}
