package com.pgwhalen.postka.clients.consumer;

import com.pgwhalen.postka.common.TopicPartition;

import java.util.Collection;

/**
 * A no-op implementation of {@link ConsumerRebalanceListener} that does nothing.
 * Used as the default listener when no custom listener is provided.
 */
final class NoOpRebalanceListener implements ConsumerRebalanceListener {

    static final NoOpRebalanceListener INSTANCE = new NoOpRebalanceListener();

    private NoOpRebalanceListener() {
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // No-op
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // No-op
    }
}
