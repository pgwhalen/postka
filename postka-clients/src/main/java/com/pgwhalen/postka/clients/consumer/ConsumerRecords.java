package com.pgwhalen.postka.clients.consumer;

import com.pgwhalen.postka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A container that holds the list of ConsumerRecord per partition for a particular topic.
 *
 * @param <K> The type of the key
 * @param <V> The type of the value
 */
public class ConsumerRecords<K, V> implements Iterable<ConsumerRecord<K, V>> {

    @SuppressWarnings("rawtypes")
    public static final ConsumerRecords EMPTY = new ConsumerRecords<>(Collections.emptyMap());

    private final Map<TopicPartition, List<ConsumerRecord<K, V>>> records;

    public ConsumerRecords(Map<TopicPartition, List<ConsumerRecord<K, V>>> records) {
        this.records = records;
    }

    /**
     * Get just the records for the given partition.
     *
     * @param partition The partition to get records for
     */
    public List<ConsumerRecord<K, V>> records(TopicPartition partition) {
        List<ConsumerRecord<K, V>> recs = records.get(partition);
        return recs == null ? Collections.emptyList() : Collections.unmodifiableList(recs);
    }

    /**
     * Get just the records for the given topic.
     *
     * @param topic The topic to get records for
     */
    public Iterable<ConsumerRecord<K, V>> records(String topic) {
        List<ConsumerRecord<K, V>> result = new ArrayList<>();
        for (Map.Entry<TopicPartition, List<ConsumerRecord<K, V>>> entry : records.entrySet()) {
            if (entry.getKey().topic().equals(topic)) {
                result.addAll(entry.getValue());
            }
        }
        return result;
    }

    /**
     * Get the set of partitions with data in this record set.
     */
    public Set<TopicPartition> partitions() {
        return Collections.unmodifiableSet(records.keySet());
    }

    /**
     * The number of records for all topics.
     */
    public int count() {
        int count = 0;
        for (List<ConsumerRecord<K, V>> recs : records.values()) {
            count += recs.size();
        }
        return count;
    }

    /**
     * Returns true if this record set contains no records.
     */
    public boolean isEmpty() {
        return records.isEmpty();
    }

    @Override
    public Iterator<ConsumerRecord<K, V>> iterator() {
        return records.values().stream()
                .flatMap(List::stream)
                .iterator();
    }

    @SuppressWarnings("unchecked")
    public static <K, V> ConsumerRecords<K, V> empty() {
        return (ConsumerRecords<K, V>) EMPTY;
    }
}
