package com.pgwhalen.postka.common;

/**
 * Information about a topic partition.
 */
public class PartitionInfo {
    private final String topic;
    private final int partition;
    private final Node leader;
    private final Node[] replicas;
    private final Node[] inSyncReplicas;

    public PartitionInfo(String topic, int partition, Node leader, Node[] replicas, Node[] inSyncReplicas) {
        this.topic = topic;
        this.partition = partition;
        this.leader = leader;
        this.replicas = replicas;
        this.inSyncReplicas = inSyncReplicas;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public Node leader() {
        return leader;
    }

    public Node[] replicas() {
        return replicas;
    }

    public Node[] inSyncReplicas() {
        return inSyncReplicas;
    }

    @Override
    public String toString() {
        return String.format("Partition(topic = %s, partition = %d, leader = %s)",
                topic, partition, leader);
    }
}
