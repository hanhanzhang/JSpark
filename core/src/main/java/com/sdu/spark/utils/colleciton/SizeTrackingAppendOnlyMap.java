package com.sdu.spark.utils.colleciton;

/**
 * 添加数据估算内存增长量
 *
 * @author hanhan.zhang
 * */
public class SizeTrackingAppendOnlyMap<K, V> extends AppendOnlyMap<K, V> {

    private SizeTracker sizeTracker = new SizeTracker(this);

    @Override
    public void update(K key, V value) {
        super.update(key, value);
        sizeTracker.afterUpdate();
    }

    @Override
    public V changeValue(K key, Updater<V> updater) {
        V newValue = super.changeValue(key, updater);
        sizeTracker.afterUpdate();
        return newValue;
    }

    @Override
    void growTable() {
        super.growTable();
        sizeTracker.resetSamples();
    }

    public long estimateSize() {
        return sizeTracker.estimateSize();
    }
}