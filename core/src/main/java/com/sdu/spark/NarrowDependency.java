package com.sdu.spark;

import com.sdu.spark.rdd.RDD;

/**
 * @author hanhan.zhang
 * */
public abstract class NarrowDependency<T> extends Dependency<T>{

    protected RDD<T> rdd;

    public NarrowDependency(RDD<T> rdd) {
        this.rdd = rdd;
    }

    @Override
    public RDD<T> rdd() {
        return rdd;
    }

    /**
     * Get the parent partitions for a child partition
     * @param partitionId a partition of the child RDD
     * @return the partitions of the parent RDD that the child partition depends upon
     * */
    public abstract int[] getParents(int partitionId);
}
