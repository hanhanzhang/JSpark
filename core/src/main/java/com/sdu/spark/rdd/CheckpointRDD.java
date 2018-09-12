package com.sdu.spark.rdd;

import com.sdu.spark.Partition;
import com.sdu.spark.SparkContext;
import com.sdu.spark.TaskContext;
import com.sdu.spark.utils.TIterator;

/**
 * @author hanhan.zhang
 * */
public abstract class CheckpointRDD<T> extends RDD<T> {

    public CheckpointRDD(SparkContext sc) {
        super(sc, null);
    }

    @Override
    public void doCheckpoint() {}

    @Override
    public void checkpoint() {}

    @Override
    public CheckpointRDD<T> localCheckpoint() {
        return this;
    }

    @Override
    public Partition[] getPartitions() {
        // 子类实现
        throw new UnsupportedOperationException("CheckpointRDD unsupported getPartitions.");
    }

    @Override
    public TIterator<T> compute(Partition split, TaskContext context) {
        // 子类实现
        throw new UnsupportedOperationException("CheckpointRDD unsupported compute.");
    }
}
