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
    private boolean keyOrdering;
    private Aggregator<K, V, C> aggregator;
    private boolean mapSideCombine;
    private int shuffleId;

    public ShuffleDependency(RDD<Product2<K, V>> rdd,
                             Partitioner partitioner) {
        this(rdd, partitioner, SparkEnv.env.serializer, false, null, true);
    }

    public ShuffleDependency(RDD<Product2<K, V>> rdd,
                             Partitioner partitioner,
                             Serializer serializer,
                             boolean keyOrdering,
                             Aggregator<K, V, C> aggregator,
                             boolean mapSideCombine) {
        this.rdd = rdd;
        this.partitioner = partitioner;
        this.serializer = serializer;
        this.keyOrdering = keyOrdering;
        this.aggregator = aggregator;
        this.mapSideCombine = mapSideCombine;

        // TODO:  _rdd.sparkContext.cleaner.foreach(_.registerShuffleForCleanup(this))
    }

    @Override
    public RDD<Product2<K, V>> rdd() {
        return null;
    }

    public int shuffleId() {
        this.shuffleId = rdd.context().newShuffleId();
        return this.shuffleId;
    }

    public ShuffleHandle shuffleHandle() {
        return SparkEnv.env.shuffleManager.registerShuffle(shuffleId, rdd().partitions().size(), this);
    }
}
