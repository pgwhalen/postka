package com.pgwhalen.postka.clients.consumer;

import com.pgwhalen.postka.common.TopicPartition;

import java.util.Collection;

/**
 * A callback interface that the user can implement to trigger custom actions when the set of
 * partitions assigned to the consumer changes.
 * <p>
 * This is applicable when the consumer is having Postka manage group membership. If the consumer
 * directly assigns partitions, this callback is not used.
 */
public interface ConsumerRebalanceListener {

    /**
     * A callback method the user can implement to provide handling of offset commits to a
     * customized store on the rebalance. This method will be called before the rebalance
     * operation starts and after the consumer stops fetching data. It is recommended that
     * offsets should be committed in this callback to either Postka or a custom offset store
     * to prevent duplicate data.
     *
     * @param partitions The list of partitions that were assigned to the consumer and now need
     *                   to be revoked (may not include all currently assigned partitions, i.e.
     *                   there may still be some partitions left)
     */
    void onPartitionsRevoked(Collection<TopicPartition> partitions);

    /**
     * A callback method the user can implement to provide handling of customized offsets on
     * completion of a successful partition re-assignment. This method will be called after the
     * rebalance operation completes and before the consumer starts fetching data.
     *
     * @param partitions The list of partitions that are now assigned to the consumer (may include
     *                   partitions previously assigned to the consumer)
     */
    void onPartitionsAssigned(Collection<TopicPartition> partitions);
}
