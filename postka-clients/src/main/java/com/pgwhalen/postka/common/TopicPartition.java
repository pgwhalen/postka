package com.pgwhalen.postka.common;

import java.util.Objects;

/**
 * A topic name and partition number.
 */
public final class TopicPartition {
    private final String topic;
    private final int partition;

    public TopicPartition(String topic, int partition) {
        this.topic = Objects.requireNonNull(topic, "topic cannot be null");
        this.partition = partition;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TopicPartition that)) return false;
        return partition == that.partition && topic.equals(that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition);
    }

    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}
