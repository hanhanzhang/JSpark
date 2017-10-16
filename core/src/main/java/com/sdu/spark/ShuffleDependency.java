package com.sdu.spark;

import com.sdu.spark.rdd.RDD;
import com.sdu.spark.serializer.Serializer;
import com.sdu.spark.shuffle.ShuffleHandle;
import com.sdu.spark.utils.scala.Product2;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @author hanhan.zhang
 * */
public class ShuffleDependency<K, V, C> extends Dependency<Product2<K, V>> {

    public transient RDD<Product2<K, V>> rdd;
    public Partitioner partitioner;
    private Serializer serializer;
    public int shuffleId;


    @Override
    public RDD<Product2<K, V>> rdd() {
        return null;
    }

    public ShuffleHandle shuffleHandle() {
        return SparkEnv.env.shuffleManager.registerShuffle(shuffleId, rdd().partitions().size(), this);
    }
}
