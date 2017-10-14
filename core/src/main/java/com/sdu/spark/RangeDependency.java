package com.sdu.spark;

import com.sdu.spark.rdd.RDD;

/**
 * @author hanhan.zhang
 * */
public class RangeDependency<T> extends NarrowDependency<T>{

    private int inStart;
    private int outStart;
    private int length;

    /**
     * @param rdd the parent RDD
     * @param inStart the start of the range in the parent RDD
     * @param outStart the start of the range in the child RDD
     * @param length the length of the range
     * */
    public RangeDependency(RDD<T> rdd, int inStart, int outStart, int length) {
        super(rdd);
        this.inStart = inStart;
        this.outStart = outStart;
        this.length = length;
    }

    @Override
    public int[] getParents(int partitionId) {
        if (partitionId >= outStart && partitionId < outStart + length) {
            // 偏移量: outStart - partitionId
            return new int[]{inStart + partitionId - outStart};
        }
        return new int[0];
    }
}
