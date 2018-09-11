package com.sdu.spark.rdd;

import com.google.common.collect.Lists;
import com.sdu.spark.*;
import com.sdu.spark.storage.BlockId.RDDBlockId;
import com.sdu.spark.storage.StorageLevel;
import com.sdu.spark.utils.CallSite;
import com.sdu.spark.utils.TIterator;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * @author hanhan.zhang
 * */
@SuppressWarnings("unchecked")
public abstract class RDD<T> implements Serializable {

    private transient SparkContext sc;
    private transient List<Dependency<?>> dependencies;

    public int id;

    private transient Partition[] partitions;
    public CallSite creationSite;
    public StorageLevel storageLevel = StorageLevel.NONE;

    private RDDCheckpointData<T> checkpointData = null;

    public RDD(SparkContext sc, List<Dependency<?>> dependencies) {
        this.sc = sc;
        this.id = this.sc.newRddId();
        this.creationSite = this.sc.getCallSite();
        this.dependencies = dependencies;
    }

    public RDD(RDD<?> oneParent) {
        this(oneParent.sc, Lists.newArrayList(new OneToOneDependency<>(oneParent)));
    }

    /** 读取分区数据, 返回该分区数据遍历器 */
    public final TIterator<T> iterator(Partition split, TaskContext context) {
        if (storageLevel != StorageLevel.NONE) {
            return getOrCompute(split, context);
        } else {
            return computeOrReadCheckpoint(split, context);
        }
    }

    protected <U> RDD<U> firstParent() {
        return (RDD<U>) dependencies().poll().rdd();
    }

    private TIterator<T> computeOrReadCheckpoint(Partition split, TaskContext context) {
        // TODO: 待实现
        throw new RuntimeException("");
    }

    private TIterator<T> getOrCompute(Partition partition, TaskContext context) {
        RDDBlockId blockId = new RDDBlockId(id, partition.index());
        // TODO: 待实现
        throw new RuntimeException("");
    }

    public SparkContext context() {
        return sc;
    }

    public final LinkedList<Dependency<?>> dependencies() {
        // TODO: 待实现
        throw new UnsupportedOperationException("");
    }

    public String[] preferredLocations(Partition split) {
        throw new UnsupportedOperationException("");
    }

    public List<RDD> getNarrowAncestors() {
        throw new UnsupportedOperationException("");
    }

    public void doCheckpoint() {

    }

    private boolean isCheckpointedAndMaterialized() {
        return checkpointData != null && checkpointData.isCheckpointed();
    }

    public abstract Partition[] partitions();

    public abstract TIterator<T> compute(Partition split, TaskContext context);
}
