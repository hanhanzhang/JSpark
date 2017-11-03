package com.sdu.spark.utils.colleciton;

import com.sdu.spark.utils.scala.Tuple2;

import java.util.Comparator;
import java.util.Iterator;

/**
 * {@link PartitionedAppendOnlyMap}职责:
 *
 *  1: 聚合同一分区下相同Key的Value值
 *
 *  2: Key的聚合操作由{@link com.sdu.spark.Aggregator}实现
 *
 * @author hanhan.zhang
 * */
public class PartitionedAppendOnlyMap<K, V> extends SizeTrackingAppendOnlyMap<Tuple2<Integer, K>, V> implements WritablePartitionedPairCollection<K, V>{

    @Override
    public void insert(int partition, K key, V value) {
        update(new Tuple2<>(partition, key), value);
    }

    @Override
    public Iterator<Tuple2<Tuple2<Integer, K>, V>> partitionedDestructiveSortedIterator(Comparator<K> keyComparator) {
        /**优先按照分区排序, 然后按照Key排序*/
        Comparator<Tuple2<Integer, K>> comparator = keyComparator != null ? partitionKeyComparator(keyComparator)
                                                                          : partitionComparator();

        return destructiveSortedIterator(comparator);
    }
}
