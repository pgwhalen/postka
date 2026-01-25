package com.pgwhalen.postka.common.header;

/**
 * The headers for a record.
 */
public interface Headers extends Iterable<Header> {

    /**
     * Adds a header to the end of the header list.
     */
    Headers add(Header header);

    /**
     * Adds a header to the end of the header list.
     */
    Headers add(String key, byte[] value);

    /**
     * Removes all headers with the given key.
     */
    Headers remove(String key);

    /**
     * Returns the last header with the given key, or null if not found.
     */
    Header lastHeader(String key);

    /**
     * Returns all headers with the given key.
     */
    Iterable<Header> headers(String key);

    /**
     * Returns all headers as an array.
     */
    Header[] toArray();
}
