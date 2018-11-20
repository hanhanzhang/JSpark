package com.sdu.spark;

import com.sdu.spark.rdd.RDD;
import com.sdu.spark.serializer.Serializer;
import com.sdu.spark.shuffle.ShuffleHandle;
import com.sdu.spark.utils.scala.Product2;

import java.util.Comparator;

/**
 * @author hanhan.zhang
 * */
public class ShuffleDependency<K, V, C> extends Dependency<Product2<K, V>> {

    public transient RDD<Product2<K, V>> rdd;
    public Partitioner partitioner;
    public Serializer serializer;
    public Comparator<K> keyOrdering;
    public Aggregator<K, V, C> aggregator;
    public boolean mapSideCombine;
    private int shuffleId;

    public ShuffleDependency(RDD<Product2<K, V>> rdd, Partitioner partitioner) {
        this(rdd, partitioner, SparkEnv.env.serializer, null, null, true);
    }

    public ShuffleDependency(RDD<Product2<K, V>> rdd, Partitioner partitioner, Serializer serializer,
                             Comparator<K> keyOrdering, Aggregator<K, V, C> aggregator, boolean mapSideCombine) {
        this.rdd = rdd;
        this.partitioner = partitioner;
        this.serializer = serializer;
        this.keyOrdering = keyOrdering;
        this.aggregator = aggregator;
        this.mapSideCombine = mapSideCombine;

        this.shuffleId = rdd.context().newShuffleId();

        // TODO:  _rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))
    }

    @Override
    public RDD<Product2<K, V>> rdd() {
        return rdd;
    }

    public int shuffleId() {
        return this.shuffleId;
    }

    public ShuffleHandle shuffleHandle() {
        return SparkEnv.env.shuffleManager.registerShuffle(shuffleId, rdd().partitions().length, this);
    }
}
