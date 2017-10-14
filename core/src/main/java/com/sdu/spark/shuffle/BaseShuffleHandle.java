package com.sdu.spark.shuffle;

import com.sdu.spark.ShuffleDependency;

/**
 * @author hanhan.zhang
 * */
public class BaseShuffleHandle<K, V, C> extends ShuffleHandle {

    public int numMaps;
    public ShuffleDependency<K, V, C> dependency;

    public BaseShuffleHandle(int shuffleId,
                             int numMaps,
                             ShuffleDependency<K, V, C> dependency) {
        super(shuffleId);
        this.numMaps = numMaps;
        this.dependency = dependency;
    }
}
