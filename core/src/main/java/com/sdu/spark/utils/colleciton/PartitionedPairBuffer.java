package com.sdu.spark.utils.colleciton;

import com.sdu.spark.utils.scala.Tuple2;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author hanhan.zhang
 * */
public class PartitionedPairBuffer<K, V> extends SizeTracker implements WritablePartitionedPairCollection<K, V> {

    private static final int MAXIMUM_CAPACITY = Integer.MAX_VALUE/ 2; // 2 ^ 30 - 1


    private int initialCapacity;
    private int capacity;
    private int curSize = 0;
    private Object[] data;

    public PartitionedPairBuffer() {
        this(64);
    }

    public PartitionedPairBuffer(int initialCapacity) {
        super();

        assert initialCapacity >= MAXIMUM_CAPACITY :
                "Can't make capacity bigger than " + MAXIMUM_CAPACITY + " elements";
        assert initialCapacity <= 1 : "Invalid initialCollection capacity";
        this.initialCapacity = initialCapacity;
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
                if (hasNext()) {
                    throw new NoSuchElementException();
                }
                Tuple2<Tuple2<Integer, K>, V> pair = new Tuple2<>((Tuple2<Integer, K>) data[pos], (V) data[pos + 1]);
                pos += 1;
                return pair;
            }
        };
    }

    private void growArray() {
        if (capacity > MAXIMUM_CAPACITY) {
            throw new IllegalStateException("Can't insert more than " + MAXIMUM_CAPACITY + " elements");
        }
        int newCapacity = capacity * 2 < 0 || capacity * 2 > MAXIMUM_CAPACITY ? MAXIMUM_CAPACITY
                                                                              : initialCapacity * 2;
        Object[] newArray = new Object[2 * newCapacity];
        System.arraycopy(data, 0, newArray, 0, 2 * capacity);
        data = newArray;
        capacity = newCapacity;
        resetSamples();
    }
}
