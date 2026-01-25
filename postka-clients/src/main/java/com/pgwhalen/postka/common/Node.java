package com.pgwhalen.postka.common;

/**
 * Information about a Postka node (placeholder for API compatibility).
 */
public class Node {
    private final int id;
    private final String host;
    private final int port;

    public Node(int id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
    }

    public int id() {
        return id;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    @Override
    public String toString() {
        return host + ":" + port + " (id: " + id + ")";
    }
}
