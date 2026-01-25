package com.pgwhalen.postka.common.header;

/**
 * A key/value pair representing a header.
 */
public interface Header {
    /**
     * The header key.
     */
    String key();

    /**
     * The header value.
     */
    byte[] value();
}
