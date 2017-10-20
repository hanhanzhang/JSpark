package com.sdu.spark.shuffle.sort;

import com.sdu.spark.TaskContext;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.scheduler.MapStatus;
import com.sdu.spark.shuffle.BypassMergeSortShuffleHandle;
import com.sdu.spark.shuffle.IndexShuffleBlockResolver;
import com.sdu.spark.shuffle.ShuffleWriter;
import com.sdu.spark.storage.BlockManager;
import com.sdu.spark.utils.scala.Product2;

import java.util.Iterator;

/**
 * @author hanhan.zhang
 * */
public class BypassMergeSortShuffleWriter<K, V> implements ShuffleWriter<K, V> {

    public BypassMergeSortShuffleWriter(BlockManager blockManager,
                                        IndexShuffleBlockResolver shuffleBlockResolver,
                                        BypassMergeSortShuffleHandle<K, V> handle,
                                        int mapId,
                                        TaskContext taskContext,
                                        SparkConf conf) {

    }

    @Override
    public void write(Iterator<Product2<K, V>> records) {

    }

    @Override
    public MapStatus stop(boolean success) {
        return null;
    }
}
