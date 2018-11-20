package com.sdu.spark.shuffle;

import com.sdu.spark.ShuffleDependency;

/**
 * {@link BypassMergeSortShuffleHandle}标识在Map端数据聚合
 *
 * @author hanhan.zhang
 * */
public class BypassMergeSortShuffleHandle<K, V> extends BaseShuffleHandle<K, V, V> {

    public BypassMergeSortShuffleHandle(int shuffleId, int numMaps, ShuffleDependency<K, V, V> dependency) {
        super(shuffleId, numMaps, dependency);
    }

}
