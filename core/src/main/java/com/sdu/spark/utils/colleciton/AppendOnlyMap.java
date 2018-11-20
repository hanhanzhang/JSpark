package com.sdu.spark.utils.colleciton;

import com.google.common.hash.Hashing;
import com.sdu.spark.utils.scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

/**
 * {@link AppendOnlyMap}仅支持(k, v)键值对添加, 不支持删除操作
 *
 *  1: AppendOnlyMap扩容阈值: 0.7 * capacity(2 ^ n)
 *
 *  2: AppendOnlyMap支持最大容量: 2 ^ 29 * 0.7
 *
 *  3: AppendOnlyMap压缩后(即{@link #destructiveSortedIterator(Comparator)}调用后)不支持查询、更新操作
 *
 * @author hanhan.zhang
 * */
public class AppendOnlyMap<K, V> implements Iterable<Tuple2<K, V>>, Serializable {

    /** 申请数组长度: 2 * capacity, 最大长度为 1 << 29 */
    private static final int MAXIMUM_CAPACITY = 1 << 29;
    /** 扩容因子(Map扩容代价: key需重新hash计算存储位置) */
    private static final float LOAD_FACTOR = 0.7f;

    /** 存储元素数量(2^n) */
    private int capacity;
    /** mask = capacity - 1, 计算hash值 */
    private int mask;
    /** 已存储元素数量 */
    private int curSize;
    /** 扩容阈值: capacity * LOAD_FACTOR */
    private int growThreshold;
    /** 存储Key-Value(偶数索引存储key, 奇数索引存储value), data.length = capacity * 2 */
    private Object[] data;

    private boolean haveNullValue = false;
    private V nullValue = null;

    /** 压缩标识, {@link #destructiveSortedIterator(Comparator)}调用后插入、更新操作不支持 */
    private boolean destroyed = false;
    private String destructionMessage = "Map state is invalid from destructive sorting!";

    public AppendOnlyMap() {
        this(64);
    }

    public AppendOnlyMap(int initialCapacity) {
        checkArgument(initialCapacity <= MAXIMUM_CAPACITY, format("Can't make capacity bigger than %d elements", MAXIMUM_CAPACITY));
        checkArgument(initialCapacity >= 1, "Invalid initial capacity");

        this.capacity = nextPowerOf2(initialCapacity);
        this.mask = capacity -1;
        this.curSize = 0;
        this.growThreshold = (int) (this.capacity * LOAD_FACTOR);
        this.data = new Object[this.capacity * 2];
    }

    /** Get the value for a given key */
    @SuppressWarnings("unchecked")
    public V apply(K key) {
        checkArgument(!destroyed, destructionMessage);
        if (key == null) {
            return nullValue;
        }

        int pos = rehash(key.hashCode()) & mask;
        int i = 1;
        while (true) {
            K curKey = (K) data[2 * pos];
            if (key.equals(curKey)) {
                return (V) data[2 * pos + 1];
            } else if (curKey == null) {
                return null;
            } else {
                int delta = i;
                pos = (pos + delta) & mask;
                i += 1;
            }
        }
    }

    /** Set the value for a key */
    @SuppressWarnings("unchecked")
    public void update(K key, V value) {
        checkArgument(!destroyed, destructionMessage);

        if (key == null) {
            if (!haveNullValue) {
                incrementSize();
            }
            nullValue = value;
            haveNullValue = true;
            return;
        }

        int pos = rehash(key.hashCode()) & mask;
        int i = 0;
        while (true) {
            K curKey = (K) data[2 * pos];
            if (curKey == null) {
                data[2 * pos] = key;
                data[2 * pos + 1] = value;
                incrementSize();
                return;
            } else if (curKey.equals(key)) {
                data[2 * pos + 1] = value;
                break;
            } else {
                int delta = i;
                pos = (pos + delta) & mask;
                i += 1;
            }
        }
    }

    /**
     * Set the value for key to valueUpdate(hadValue, oldValue), where oldValue will be the old value
     * for key, if any, or null otherwise. Returns the newly updated value.
     */
    @SuppressWarnings("unchecked")
    public V changeValue(K key, Updater<V> updater) {
        checkArgument(!destroyed, destructionMessage);
        if (key == null) {
            if (!haveNullValue) {
                incrementSize();
            }
            nullValue = updater.valueUpdate(haveNullValue, nullValue);
            haveNullValue = true;
            return nullValue;
        }

        int pos = rehash(key.hashCode()) & mask;
        int i = 0;
        while (true) {
            K curKey = (K) data[pos * 2];
            if (curKey == null) {       // key不存在, value肯定为null
                V newValue = updater.valueUpdate(false, nullValue);
                data[2 * pos] = key;
                data[2 * pos + 1] = newValue;
                incrementSize();
                return newValue;
            } else if (curKey.equals(key)) {
                V newValue = updater.valueUpdate(true, (V) data[2 * pos + 1]);
                data[2 * pos + 1] = newValue;
                return newValue;
            } else {
                int delta = i;
                pos = (pos + delta) & mask;
                i += 1;
            }
        }
    }

    private void incrementSize() {
        curSize++;
        if (curSize > growThreshold) {
            growTable();
        }
    }

    private int rehash(int h) {
        return Hashing.murmur3_32().hashInt(h).asInt();
    }

    @SuppressWarnings("unchecked")
    void growTable() {
        // capacity < MAXIMUM_CAPACITY (2 ^ 29) so capacity * 2 won't overflow
        int newCapacity = capacity * 2;
        checkArgument(newCapacity <= MAXIMUM_CAPACITY, "Can't contain more than " + MAXIMUM_CAPACITY + " elements");

        Object[] newData = new Object[newCapacity * 2];
        int newMask = newCapacity - 1;
        // Insert all our old values into the new array. Note that because our old keys are
        // unique, there's no need to check for equality here when we insert.
        int oldPos = 0;
        while (oldPos < capacity) {
            if (data[oldPos * 2] != null) {
                K key = (K) data[2* oldPos];
                V value = (V) data[2 * oldPos + 1];
                int newPos = rehash(key.hashCode()) & newMask;
                int i = 1;
                boolean keepGoing = true;
                // 解决hash冲突
                while (keepGoing) {
                    K curKey = (K) newData[2 * newPos];
                    if (curKey == null) {
                        newData[2 * newPos] = key;
                        newData[2 * newPos + 1] = value;
                        keepGoing = false;
                    } else {
                        int delta = i;
                        newPos = (newPos + delta) & newMask;
                        i += 1;
                    }
                }
            }
            oldPos += 1;
        }

        // 赋值
        data = newData;
        capacity = newCapacity;
        mask = newMask;
        growThreshold = (int) (LOAD_FACTOR * newCapacity);
    }

    private boolean atGrowThreshold() {
        return curSize == growThreshold;
    }

    @Override
    public Iterator<Tuple2<K, V>> iterator() {
        return new Iterator<Tuple2<K, V>>() {
            int pos = -1;

            @SuppressWarnings("unchecked")
            private Tuple2<K, V> nextValue() {
                if (pos == -1) {
                    if (haveNullValue) {
                        return new Tuple2<>(null, nullValue);
                    }
                    pos += 1;
                }
                while (pos < capacity) {
                    if (data[2 * pos + 1] != null) {
                        return new Tuple2<>((K) data[2 * pos], (V) data[2 * pos + 1]);
                    }
                    ++pos;
                }
                return null;
            }

            @Override
            public boolean hasNext() {
                return nextValue() != null;
            }

            @Override
            public Tuple2<K, V> next() {
                Tuple2<K, V> value = nextValue();
                if (value == null) {
                    throw new NoSuchElementException("End of iterator");
                }
                pos += 1;
                return value;
            }
        };
    }

    /**
     * Return an iterator of the map in sorted order. This provides a way to sort the map without
     * using additional memory, at the expense of destroying the validity of the map.
     */
    public Iterator<Tuple2<K, V>> destructiveSortedIterator(Comparator<K> keyComparator) {
        destroyed = true;

        // 数组压缩
        int index = 0,  newIndex = 0;
        while (index < capacity) {
            if (data[2 * index] != null) {
                data[2 * newIndex] = data[2 * index];
                data[2 * newIndex + 1] = data[2 * index + 1];
                ++newIndex;
            }
            ++index;
        }

        assert curSize == (newIndex + (haveNullValue ? 1 : 0));

        final int maxIndex = newIndex;
        // 排序
        new Sorter<K, Object[]>(new KVArraySortDataFormat<>()).sort(data, 0, newIndex, keyComparator);

        return new Iterator<Tuple2<K, V>>() {
            int i = 0;
            boolean nullValueReady = haveNullValue;

            @Override
            public boolean hasNext() {
                return i < maxIndex || nullValueReady;
            }

            @SuppressWarnings("unchecked")
            @Override
            public Tuple2<K, V> next() {
                if (nullValueReady) {
                    nullValueReady = false;
                    return new Tuple2<>(null, nullValue);
                } else {
                    Tuple2<K, V> item = new Tuple2<>((K) data[i * 2], (V) data[i * 2 + 1]);
                    ++i;
                    return item;
                }
            }
        };
    }

    // for test
    public int capacity() {
        return capacity;
    }

    public int size() {
        return curSize;
    }

    private static int nextPowerOf2(int n) {
        int highBit = Integer.highestOneBit(n);
        return n == highBit ? n : highBit << 1;
    }

    public interface Updater<V> {
        V valueUpdate(boolean hadValue, V value);
    }

    public static void main(String[] args) {
        AppendOnlyMap<String, Integer> map = new AppendOnlyMap<>(2);
        map.update("A", 65);
        map.update("B", 66);
        map.update("C", 67);

        System.out.println(map.apply("A"));
        System.out.println(map.apply("C"));

        Updater<Integer> updater = (hadValue, oldValue) -> hadValue ? oldValue + 1 : 0;

        map.changeValue("A", updater);
        map.changeValue("D", updater);

        Iterator<Tuple2<String, Integer>> iterator = map.destructiveSortedIterator(Comparator.naturalOrder());
        while (iterator.hasNext()) {
            Tuple2<String, Integer> t = iterator.next();
            System.out.println("Key: " + t._1() + ", Value: " + t._2());
        }

        // map不在支持apply、update
//        System.out.println(map.apply("B"));
    }
}
