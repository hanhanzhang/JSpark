package com.sdu.spark.shuffle;

import com.sdu.spark.ShuffleDependency;
import com.sdu.spark.TaskContext;

/**
 * @author hanhan.zhang
 * */
public interface ShuffleManager {

    /**
     * Register a shuffle with the manager and obtain a handle for it to pass to tasks.
     * */
    <K, V, C> ShuffleHandle registerShuffle(int shuffleId,
                                            int numMaps,
                                            ShuffleDependency<K, V, C> dependency);

    /**
     * Get a writer for a given partition. Called on executors by map tasks.
     * */
    <K, V> ShuffleWriter<K, V> getWriter(ShuffleHandle handle,
                                         int mapId,
                                         TaskContext context);
    /**
     * Get a reader for a range of reduce partitions (startPartition to endPartition-1, inclusive).
     * Called on executors by reduce tasks.
     */
    <K, V> ShuffleReader<K, V> getReader(ShuffleHandle handle,
                                         int startPartition,
                                         int endPartition,
                                         TaskContext context);

    /**
     * Remove a shuffle's metadata from the ShuffleManager.
     * @return true if the metadata removed successfully, otherwise false.
     */
    boolean unregisterShuffle(int shuffleId);

    /**
     * Return a resolver capable of retrieving shuffle block data based on block coordinates.
     */
    ShuffleBlockResolver shuffleBlockResolver();

    void stop();
}