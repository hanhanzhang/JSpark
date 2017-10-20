package com.sdu.spark.shuffle.sort;

import com.sdu.spark.TaskContext;
import com.sdu.spark.memory.TaskMemoryManager;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.scheduler.MapStatus;
import com.sdu.spark.shuffle.IndexShuffleBlockResolver;
import com.sdu.spark.shuffle.SerializedShuffleHandle;
import com.sdu.spark.shuffle.ShuffleWriter;
import com.sdu.spark.storage.BlockManager;
import com.sdu.spark.utils.scala.Product2;

import java.io.IOException;
import java.util.Iterator;

/**
 * TODO: 待开发
 *
 * @author hanhan.zhang
 * */
public class UnsafeShuffleWriter<K, V> implements ShuffleWriter<K, V> {


    public UnsafeShuffleWriter(
            BlockManager blockManager,
            IndexShuffleBlockResolver shuffleBlockResolver,
            TaskMemoryManager memoryManager,
            SerializedShuffleHandle<K, V> handle,
            int mapId,
            TaskContext taskContext,
            SparkConf sparkConf) {

    }

    @Override
    public void write(Iterator<Product2<K, V>> records) {

    }

    @Override
    public MapStatus stop(boolean success) {
        return null;
    }
}
