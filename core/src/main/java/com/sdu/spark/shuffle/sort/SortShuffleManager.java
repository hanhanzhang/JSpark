package com.sdu.spark.shuffle.sort;

import com.sdu.spark.ShuffleDependency;
import com.sdu.spark.TaskContext;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.shuffle.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author hanhan.zhang
 * */
public class SortShuffleManager implements ShuffleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortShuffleManager.class);

    private SparkConf conf;
    private IndexShuffleBlockResolver shuffleBlockResolver;

    public SortShuffleManager(SparkConf conf) {
        this.conf = conf;
        this.shuffleBlockResolver = new IndexShuffleBlockResolver(conf);

        if (!conf.getBoolean("spark.shuffle.spill", true)) {
            LOGGER.warn("spark.shuffle.spill was set to false, but this configuration is ignored as of Spark 1.6+." +
                        " Shuffle will continue to spill to disk when necessary.");
        }
    }

    @Override
    public <K, V, C> ShuffleHandle registerShuffle(int shuffleId, int numMaps, ShuffleDependency<K, V, C> dependency) {
        return null;
    }

    @Override
    public <K, V> ShuffleWriter<K, V> getWriter(ShuffleHandle handle, int mapId, TaskContext context) {
        return null;
    }

    @Override
    public <K, V> ShuffleReader<K, V> getReader(ShuffleHandle handle, int startPartition, int endPartition, TaskContext context) {
        return null;
    }

    @Override
    public boolean unregisterShuffle(int shuffleId) {
        return false;
    }

    @Override
    public ShuffleBlockResolver shuffleBlockResolver() {
        return null;
    }

    @Override
    public void stop() {

    }
}
