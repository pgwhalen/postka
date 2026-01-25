package com.pgwhalen.postka.clients.consumer;

/**
 * Configuration for the Postka consumer.
 */
public class ConsumerConfig {
    /**
     * The DataSource to use for database connections.
     */
    public static final String DATASOURCE_CONFIG = "postka.datasource";

    /**
     * A unique string that identifies the consumer group this consumer belongs to.
     */
    public static final String GROUP_ID_CONFIG = "group.id";

    /**
     * A list of host/port pairs to use for establishing the initial connection.
     * This is used for API compatibility with Kafka but is not used by Postka.
     */
    public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";

    /**
     * An id string to pass to the server when making requests.
     */
    public static final String CLIENT_ID_CONFIG = "client.id";

    /**
     * What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server.
     */
    public static final String AUTO_OFFSET_RESET_CONFIG = "auto.offset.reset";

    /**
     * If true the consumer's offset will be periodically committed in the background.
     */
    public static final String ENABLE_AUTO_COMMIT_CONFIG = "enable.auto.commit";

    private ConsumerConfig() {
    }
}
