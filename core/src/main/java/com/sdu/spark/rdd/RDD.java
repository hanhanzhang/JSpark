package com.sdu.spark.rdd;

import com.sdu.spark.*;
import com.sdu.spark.storage.BlockId.RDDBlockId;
import com.sdu.spark.storage.BlockResult;
import com.sdu.spark.storage.StorageLevel;
import com.sdu.spark.utils.CallSite;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author hanhan.zhang
 * */
public abstract class RDD<T> implements Serializable {

    private transient SparkContext sc;
    private transient List<Dependency<?>> deps;

    public int id;

    private transient List<Partition> partitions;
    public CallSite creationSite;
    public StorageLevel storageLevel = StorageLevel.NONE;

    private RDDCheckpointData<T> checkpointData = null;

    public RDD(SparkContext sc, List<Dependency<?>> deps) {
        this.sc = sc;
        this.id = this.sc.newRddId();
        this.creationSite = this.sc.getCallSite();
        this.deps = deps;
    }

    public final Iterator<T> iterator(Partition split, TaskContext context) {
        if (storageLevel != StorageLevel.NONE) {
            return getOrCompute(split, context);
        } else {
            return computeOrReadCheckpoint(split, context);
        }
    }

    @SuppressWarnings("unchecked")
    protected <U> RDD<U> firstParent() {
        return (RDD<U>) dependencies().poll().rdd();
    }

    @SuppressWarnings("unchecked")
    private Iterator<T> computeOrReadCheckpoint(Partition split, TaskContext context) {
        if (isCheckpointedAndMaterialized()) {
            return (Iterator<T>) firstParent().iterator(split, context);
        } else {
            return compute(split, context);
        }
    }

    @SuppressWarnings("unchecked")
    private Iterator<T> getOrCompute(Partition partition, TaskContext context) {
        RDDBlockId blockId = new RDDBlockId(id, partition.index());
        Pair<BlockResult, Iterator<T>> res = SparkEnv.env.blockManager.getOrElseUpdate(blockId, storageLevel, () ->
            computeOrReadCheckpoint(partition, context)
        );
        if (res.getLeft() != null) {
            return new InterruptibleIterator<>(context, (Iterator<T>) res.getLeft().data);
        } else if (res.getRight() != null) {
            return new InterruptibleIterator<>(context, res.getRight());
        }
        throw new SparkException(String.format("Got partition %d of rdd %s failure", partition.index(), blockId));
    }

    public final LinkedList<Dependency<?>> dependencies() {
        throw new UnsupportedOperationException("");
    }

    public List<Partition> partitions() {
        throw new UnsupportedOperationException("");
    }

    public List<RDD> getNarrowAncestors() {
        throw new UnsupportedOperationException("");
    }

    private boolean isCheckpointedAndMaterialized() {
        return checkpointData != null && checkpointData.isCheckpointed();
    }

    public abstract Iterator<T> compute(Partition split, TaskContext context);
}
