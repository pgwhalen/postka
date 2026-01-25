package com.pgwhalen.postka.common.header;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A mutable list of record headers.
 */
public class RecordHeaders implements Headers {
    private final List<Header> headers;

    public RecordHeaders() {
        this.headers = new ArrayList<>();
    }

    public RecordHeaders(Iterable<Header> headers) {
        this.headers = new ArrayList<>();
        if (headers != null) {
            for (Header header : headers) {
                this.headers.add(header);
            }
        }
    }

    @Override
    public Headers add(Header header) {
        headers.add(header);
        return this;
    }

    @Override
    public Headers add(String key, byte[] value) {
        return add(new RecordHeader(key, value));
    }

    @Override
    public Headers remove(String key) {
        headers.removeIf(h -> h.key().equals(key));
        return this;
    }

    @Override
    public Header lastHeader(String key) {
        for (int i = headers.size() - 1; i >= 0; i--) {
            Header header = headers.get(i);
            if (header.key().equals(key)) {
                return header;
            }
        }
        return null;
    }

    @Override
    public Iterable<Header> headers(String key) {
        List<Header> result = new ArrayList<>();
        for (Header header : headers) {
            if (header.key().equals(key)) {
                result.add(header);
            }
        }
        return result;
    }

    @Override
    public Header[] toArray() {
        return headers.toArray(new Header[0]);
    }

    @Override
    public Iterator<Header> iterator() {
        return headers.iterator();
    }
}
