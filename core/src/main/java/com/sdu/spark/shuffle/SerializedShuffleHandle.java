package com.sdu.spark.shuffle;

import com.sdu.spark.ShuffleDependency;

/**
 * {@link SerializedShuffleHandle}标识Map端Shuffle数据可序列化
 *
 * @author hanhan.zhang
 * */
public class SerializedShuffleHandle<K, V> extends BaseShuffleHandle<K, V, V> {

    public SerializedShuffleHandle(int shuffleId,
                                   int numMaps,
                                   ShuffleDependency<K, V, V> dependency) {
        super(shuffleId, numMaps, dependency);
    }
    
}
