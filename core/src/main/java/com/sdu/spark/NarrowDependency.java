package com.sdu.spark;

import com.sdu.spark.rdd.RDD;

import java.util.List;

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

    public abstract List<Integer> getParents(int partitionId);
}
