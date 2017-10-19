package com.sdu.spark.utils.colleciton;

import com.sdu.spark.Aggregator.*;
import com.sdu.spark.SparkEnv;
import com.sdu.spark.TaskContext;
import com.sdu.spark.memory.MemoryConsumer;
import com.sdu.spark.serializer.Serializer;
import com.sdu.spark.serializer.SerializerManager;
import com.sdu.spark.storage.BlockManager;
import com.sdu.spark.utils.scala.Product2;
import com.sdu.spark.utils.scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;

/**
 * {@link ExternalAppendOnlyMap}类似Hadoop MapReduce中shuffle-merge-combine-sort过程
 *
 *
 * @author hanhan.zhang
 * */
public class ExternalAppendOnlyMap<K, V, C> extends Spillable<SizeTracker> implements Serializable, Iterable<Tuple2<K, C>> {

    private Serializer serializer;
    private BlockManager blockManager;
    private TaskContext context;
    private SerializerManager serializerManager;

    public ExternalAppendOnlyMap(CreateAggregatorInitialValue<V, C> initial,
                                 CreateAggregatorMergeValue<V, C> merge,
                                 CreateAggregatorOutput<C> output) {
        super(TaskContext.get().taskMemoryManager());
        this.serializer = SparkEnv.env.closureSerializer;
        this.blockManager = SparkEnv.env.blockManager;
        this.context = TaskContext.get();
        this.serializerManager = SparkEnv.env.serializerManager;
    }

    @Override
    public long spill(long size, MemoryConsumer trigger) throws IOException {
        return 0;
    }

    @Override
    public boolean forceSpill() {
        return false;
    }

    @Override
    public void spill(SizeTracker collection) {

    }

    public void insertAll(Iterator<? extends Product2<K, V>> entries) {
        throw new UnsupportedOperationException("");
    }

    @Override
    public Iterator<Tuple2<K, C>> iterator() {
        throw new UnsupportedOperationException("");
    }
}
