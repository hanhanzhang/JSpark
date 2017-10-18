package com.sdu.spark.shuffle.sort;

import com.sdu.spark.ShuffleDependency;
import com.sdu.spark.SparkEnv;
import com.sdu.spark.TaskContext;
import com.sdu.spark.scheduler.MapStatus;
import com.sdu.spark.shuffle.BaseShuffleHandle;
import com.sdu.spark.shuffle.IndexShuffleBlockResolver;
import com.sdu.spark.shuffle.ShuffleWriter;
import com.sdu.spark.storage.BlockManager;
import com.sdu.spark.utils.colleciton.ExternalSorter;
import com.sdu.spark.utils.scala.Product2;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Iterator;

/**
 * @author hanhan.zhang
 * */
public class SortShuffleWriter<K, V, C> implements ShuffleWriter<K, V>{

    private IndexShuffleBlockResolver shuffleBlockResolver;
    private BaseShuffleHandle<K, V, C> handle;
    private int mapId;
    private TaskContext context;

    private boolean stopping = false;
    private BlockManager blockManager;
    private ShuffleDependency<K, V, C> dep;
    private MapStatus mapStatus;

    private ExternalSorter<K, V, C> sorter;

    public SortShuffleWriter(IndexShuffleBlockResolver shuffleBlockResolver,
                             BaseShuffleHandle<K, V, C> handle,
                             int mapId,
                             TaskContext context) {
        this.shuffleBlockResolver = shuffleBlockResolver;
        this.handle = handle;
        this.mapId = mapId;
        this.context = context;

        this.dep = handle.dependency;
        this.blockManager = SparkEnv.env.blockManager;
    }

    @Override
    public void write(Iterator<Product2<K, V>> records) {

    }

    @Override
    public MapStatus stop(boolean success) {
        try {
            if (stopping) {
                return null;
            }
            stopping = true;
            if (success) {
                return mapStatus;
            } else {
                return null;
            }
        } finally {
            if (sorter != null) {
                sorter.stop();
                sorter = null;
            }
        }
    }
}
