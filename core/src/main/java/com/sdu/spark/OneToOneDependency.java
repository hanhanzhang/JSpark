package com.sdu.spark;

import com.sdu.spark.rdd.RDD;

/**
 * @author hanhan.zhang
 * */
public class OneToOneDependency<T> extends NarrowDependency<T> {

    public OneToOneDependency(RDD<T> rdd) {
        super(rdd);
    }

    @Override
    public int[] getParents(int partitionId) {
        return new int[]{partitionId};
    }


}
