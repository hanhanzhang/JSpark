package com.sdu.spark;

import com.sdu.spark.rdd.RDD;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @author hanhan.zhang
 * */
public class ShuffleDependency<K, V, C> extends Dependency<Pair<K, V>> {

    public int shuffleId;

    @Override
    public RDD<Pair<K, V>> rdd() {
        return null;
    }
}
