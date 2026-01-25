package com.pgwhalen.postka.clients.producer;

/**
 * Configuration for the Postka producer.
 */
public class ProducerConfig {
    /**
     * The DataSource to use for database connections.
     */
    public static final String DATASOURCE_CONFIG = "postka.datasource";

    /**
     * A list of host/port pairs to use for establishing the initial connection.
     * This is used for API compatibility with Kafka but is not used by Postka.
     */
    public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";

    /**
     * An id string to pass to the server when making requests.
     */
    public static final String CLIENT_ID_CONFIG = "client.id";

    private ProducerConfig() {
    }
}
