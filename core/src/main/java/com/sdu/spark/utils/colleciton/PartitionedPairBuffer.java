package com.sdu.spark.utils.colleciton;

import com.sdu.spark.utils.scala.Tuple2;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.sdu.spark.utils.colleciton.WritablePartitionedPairCollections.partitionComparator;
import static com.sdu.spark.utils.colleciton.WritablePartitionedPairCollections.partitionKeyComparator;
import static java.lang.String.format;

/**
 * PartitionedPairBuffer、PartitionedAppendOnlyMap异同:
 *
 * 1: 两者底层均使用数据, 存储结构如下:
 *
 *    +------------------+------------+------------------+-------------+------------------+------------+
 *    | (partitionId, K) |     V      | (partitionId, K) |      V      | (partitionId, K) |     V      |
 *    +------------------+------------+------------------+-------------+------------------+------------+
 *
 * 2: 区别
 *
 *    +--------------------------+-------------------+-------------------------------------------------+
 *    |                          | Support Aggregate |                     implement
 *    +--------------------------+-------------------+-------------------------------------------------+
 *    | PartitionedAppendOnlyMap |        yes        |  HashMap Structure, Support lookup and update   |
 *    +--------------------------+-------------------+-------------------------------------------------+
 *    |  PartitionedPairBuffer   |        no         |        Array Structure(Append Element)          |
 *    +--------------------------+-------------------+-------------------------------------------------+
 *
 * ExternalSorter对两者数据结构的选择: 若是支持聚合操作, 则选择PartitionedAppendOnlyMap(支持Update操作), 否则选择PartitionedPairBuffer
 *
 * @author hanhan.zhang
 * */
public class PartitionedPairBuffer<K, V> extends SizeTracker implements WritablePartitionedPairCollection<K, V> {

    private static final int MAXIMUM_CAPACITY = Integer.MAX_VALUE/ 2; // 2 ^ 30 - 1

    private int capacity;
    private int curSize = 0;
    private Object[] data;

    public PartitionedPairBuffer() {
        this(64);
    }

    public PartitionedPairBuffer(int initialCapacity) {
        super();
        assert initialCapacity <= MAXIMUM_CAPACITY : format("Can't make capacity bigger than %s elements", MAXIMUM_CAPACITY);
        assert initialCapacity >= 1 : "Invalid initial capacity";
        this.capacity = initialCapacity;
        this.data = new Object[2 * initialCapacity];
    }

    @Override
    public void insert(int partition, K key, V value) {
        if (curSize == capacity) {
            growArray();
        }
        data[2 * curSize] = new Tuple2<>(partition, key);
        data[2 * curSize + 1] = value;
        curSize += 1;
        afterUpdate();
    }

    @Override
    public Iterator<Tuple2<Tuple2<Integer, K>, V>> partitionedDestructiveSortedIterator(Comparator<K> keyComparator) {
        Comparator<Tuple2<Integer, K>> comparator = keyComparator != null ? partitionKeyComparator(keyComparator)
                                                                          : partitionComparator();
        // 合并排序
        new Sorter<Tuple2<Integer, K>, Object[]>(new KVArraySortDataFormat<>()).sort(data, 0, curSize, comparator);
        return iterator();
    }

    private Iterator<Tuple2<Tuple2<Integer, K>, V>> iterator() {
        return new Iterator<Tuple2<Tuple2<Integer, K>, V>>() {
            int pos = 0;

            @Override
            public boolean hasNext() {
                return pos < curSize;
            }

            @SuppressWarnings("unchecked")
            @Override
            public Tuple2<Tuple2<Integer, K>, V> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Tuple2<Tuple2<Integer, K>, V> pair = new Tuple2<>((Tuple2<Integer, K>) data[pos * 2], (V) data[pos * 2 + 1]);
                pos += 1;
                return pair;
            }
        };
    }

    private void growArray() {
        if (capacity > MAXIMUM_CAPACITY) {
            throw new IllegalStateException("Can't insert more than " + MAXIMUM_CAPACITY + " elements");
        }
        int newCapacity = capacity * 2 > MAXIMUM_CAPACITY ? MAXIMUM_CAPACITY : capacity * 2;
        Object[] newArray = new Object[2 * newCapacity];
        System.arraycopy(data, 0, newArray, 0, 2 * capacity);
        data = newArray;
        capacity = newCapacity;
        resetSamples();
    }

    public static void main(String[] args) {
        PartitionedPairBuffer<String, Integer> buffer = new PartitionedPairBuffer<>(2);
        buffer.insert(1, "A", 10);
        buffer.insert(1, "B", 11);
        // 扩容
        buffer.insert(2, "D", 15);
        buffer.insert(2, "E", 16);
        // 再次扩容
        buffer.insert(3, "X", 21);
        buffer.insert(3, "Y", 32);

        Iterator<Tuple2<Tuple2<Integer, String>, Integer>> iterator = buffer.partitionedDestructiveSortedIterator(null);
        while (iterator.hasNext()) {
            Tuple2<Tuple2<Integer, String>, Integer> t = iterator.next();
            System.out.println("Partition: " + t._1()._1() + ", Key: " + t._1()._2() + ", Value: " + t._2());
        }

    }
}
