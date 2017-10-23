package com.sdu.spark.utils.colleciton;

import com.sdu.spark.Aggregator;
import com.sdu.spark.Partitioner;
import com.sdu.spark.TaskContext;
import com.sdu.spark.memory.MemoryConsumer;
import com.sdu.spark.serializer.Serializer;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.utils.scala.Product2;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * @author hanhan.zhang
 * */
public class ExternalSorter<K, V, C> extends Spillable<WritablePartitionedPairCollection<K, C>> {

    private TaskContext context;
    private Aggregator<K, V, C> aggregator;
    private Partitioner partitioner;
    private boolean ordering;
    private Serializer serializer;

    public ExternalSorter(TaskContext context,
                          Aggregator<K, V, C> aggregator,
                          Partitioner partitioner,
                          boolean ordering,
                          Serializer serializer) {
        super(context.taskMemoryManager());
        this.context = context;
        this.aggregator = aggregator;
        this.partitioner = partitioner;
        this.ordering = ordering;
        this.serializer = serializer;
    }

    public void insertAll(Iterator<? extends Product2<K, V>> records) {
        throw new UnsupportedOperationException("");
    }

    public long[] writePartitionedFile(BlockId blockId, File outputFile) {
        throw new UnsupportedOperationException("");
    }

    public void stop() {
        throw new UnsupportedOperationException("");
    }

    @Override
    public boolean forceSpill() {
        return false;
    }

    @Override
    public void spill(WritablePartitionedPairCollection<K, C> collection) {

    }
}
