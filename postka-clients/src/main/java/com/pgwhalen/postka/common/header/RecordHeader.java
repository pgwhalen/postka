package com.pgwhalen.postka.common.header;

import java.util.Arrays;
import java.util.Objects;

/**
 * A key/value pair representing a record header.
 */
public class RecordHeader implements Header {
    private final String key;
    private final byte[] value;

    public RecordHeader(String key, byte[] value) {
        this.key = Objects.requireNonNull(key, "key cannot be null");
        this.value = value;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public byte[] value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RecordHeader that)) return false;
        return key.equals(that.key) && Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        int result = key.hashCode();
        result = 31 * result + Arrays.hashCode(value);
        return result;
    }

    @Override
    public String toString() {
        return "RecordHeader(key = " + key + ", value = " + Arrays.toString(value) + ")";
    }
}
