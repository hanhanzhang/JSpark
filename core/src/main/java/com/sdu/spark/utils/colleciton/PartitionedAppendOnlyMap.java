package com.sdu.spark.utils.colleciton;

import com.sdu.spark.utils.scala.Tuple2;

import java.util.Comparator;
import java.util.Iterator;

import static com.sdu.spark.utils.colleciton.WritablePartitionedPairCollections.partitionComparator;
import static com.sdu.spark.utils.colleciton.WritablePartitionedPairCollections.partitionKeyComparator;

/**
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
        Comparator<Tuple2<Integer, K>> comparator = keyComparator != null ? partitionKeyComparator(keyComparator)
                                                                          : partitionComparator();

        return destructiveSortedIterator(comparator);
    }

    public static void main(String[] args) {
        // 默认容量64
        PartitionedAppendOnlyMap<String, Integer> map = new PartitionedAppendOnlyMap<>();
        map.insert(2, "D", 21);
        map.insert(2, "E", 22);
        map.insert(2, "F", 23);

        map.insert(1, "A", 11);
        map.insert(1, "B", 12);
        map.insert(1, "C", 13);

        Iterator<Tuple2<Tuple2<Integer, String>, Integer>> iterator = map.partitionedDestructiveSortedIterator(null);
        while (iterator.hasNext()) {
            Tuple2<Tuple2<Integer, String>, Integer> t = iterator.next();
            System.out.println("Partition: " + t._1()._1() + ", Key: " + t._1()._2() + ", Value: " + t._2());
        }
    }
}
