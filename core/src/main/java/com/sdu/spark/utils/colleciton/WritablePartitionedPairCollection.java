package com.sdu.spark.utils.colleciton;

import com.sdu.spark.SparkException;
import com.sdu.spark.storage.DiskBlockObjectWriter;
import com.sdu.spark.utils.scala.Tuple2;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

/**
 * @author hanhan.zhang
 * */
public interface WritablePartitionedPairCollection<K, V> {

    void insert(int partition, K key, V value);

    Iterator<Tuple2<Tuple2<Integer, K>, V>> partitionedDestructiveSortedIterator(Comparator<K> keyComparator);

    default WritablePartitionedIterator destructiveSortedWritablePartitionedIterator(Comparator<K> keyComparator) {
        Iterator<Tuple2<Tuple2<Integer, K>, V>> it = partitionedDestructiveSortedIterator(keyComparator);
        return new WritablePartitionedIterator() {

            Tuple2<Tuple2<Integer, K>, V> cur = it.hasNext() ? it.next() : null;

            @Override
            public void writeNext(DiskBlockObjectWriter writer) {
                try {
                    writer.write(cur._1()._2(), cur._2());
                } catch (IOException e) {
                    throw new SparkException(String.format("Exception occurred when write (%s, %s) to disk", cur._1(), cur._2()), e);
                }
                cur = it.hasNext() ? it.next() : null;
            }

            @Override
            public boolean hasNext() {
                return cur != null;
            }

            @Override
            public int nextPartition() {
                return cur._1()._1();
            }
        };
    }
}
