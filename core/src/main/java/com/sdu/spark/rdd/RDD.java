package com.sdu.spark.rdd;

import com.google.common.collect.Lists;
import com.sdu.spark.*;
import com.sdu.spark.storage.BlockId.RDDBlockId;
import com.sdu.spark.storage.StorageLevel;
import com.sdu.spark.utils.CallSite;
import com.sdu.spark.utils.TIterator;

import java.io.Serializable;
import java.util.List;

import static java.lang.String.format;

/**
 * @author hanhan.zhang
 * */
@SuppressWarnings("unchecked")
public abstract class RDD<T> implements Serializable {

    private static final String CHECKPOINT_ALL_MARKED_ANCESTORS = "spark.checkpoint.checkpointAllMarkedAncestors";

    private transient SparkContext sc;
    private transient List<Dependency<?>> dependencies;

    // RDD唯一标识
    private int id;
    // RDD名称
    private transient String name;
    // RDD所有分区
    private transient Partition[] partitions;
    // RDD分区计算器
    private transient Partitioner partitioner;
    // RDD的存储级别
    private StorageLevel storageLevel = StorageLevel.NONE;
    // RDD的检查点数据
    private RDDCheckpointData<T> checkpointData = null;
    // 是否对所有标记需要保存检查点的祖先保存检查点
    private boolean checkpointAllMarkedAncestors;
    // 是否已调用doCheckpoint方法设置检查点, 防止对RDD多次设置检查点
    private transient boolean doCheckpointCalled = false;

    private CallSite creationSite;


    public RDD(SparkContext sc, List<Dependency<?>> dependencies) {
        this.sc = sc;
        this.id = this.sc.newRddId();
        this.creationSite = this.sc.getCallSite();
        this.dependencies = dependencies;

//        this.checkpointAllMarkedAncestors = sc.conf
    }

    public RDD(RDD<?> oneParent) {
        this(oneParent.sc, Lists.newArrayList(new OneToOneDependency<>(oneParent)));
    }

    /** 对RDD分区数据计算 */
    public abstract TIterator<T> compute(Partition split, TaskContext context);

    /** 获取当前RDD的所有分区 */
    public abstract Partition[] getPartitions();
    /** 获取RDD分区顺序: CheckpointRDD -> RDD */
    public final Partition[] partitions() {
        CheckpointRDD cpRDD = checkpointRDD();
        if (cpRDD == null) {
            // RDD检查点未完成
            if (partitions == null) {
                partitions = getPartitions();
                // 校验
                int index = 0;
                for (Partition p : partitions) {
                    assert p.index() == index : format("partitions[%d].partition == %s, but it should equal %s",
                            index, p.index(), index);
                    index += 1;
                }
            }
            return partitions;
        } else {
            // RDD检查点已完成, 分区数及依赖已清空, @See markCheckpointed()
            return cpRDD.partitions();
        }
    }

    /** 获取当前RDD的所有依赖 */
    public List<Dependency<?>> getDependencies() {
        return dependencies;
    }

    /** RDD依赖关系获取顺序: CheckpointRDD -> RDD依赖 */
    public final List<Dependency<?>> dependencies() {
        CheckpointRDD<?> cpRDD = checkpointRDD();
        if (cpRDD == null) {
            if (dependencies == null) {
                dependencies = getDependencies();
            }
            return dependencies;
        } else {
            // RDD检查点已完成, 分区数及依赖已清空, @See markCheckpointed()
            return Lists.newArrayList(new OneToOneDependency<>(cpRDD));
        }
    }

    /** 子类需重写 */
    public String[] getPreferredLocations(Partition split) {
        return new String[0];
    }

    public String[] preferredLocations(Partition split) {
        CheckpointRDD<T> cpRDD = checkpointRDD();
        if (cpRDD == null) {
            return getPreferredLocations(split);
        }  else {
           return cpRDD.preferredLocations(split);
        }
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
        throw new RuntimeException("");
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

    public List<RDD> getNarrowAncestors() {
        throw new UnsupportedOperationException("");
    }

    private CheckpointRDD<T> checkpointRDD() {
        return checkpointData.checkpointRDD();
    }

    public void checkpoint() {}

    public void doCheckpoint() {
    }

    public void markCheckpointed() {
        clearDependencies();
        partitions = null;
        dependencies = null;
    }

    public RDD<T> localCheckpoint() {
        throw new RuntimeException("");
    }

    public String getName() {
        return name;
    }

    public int getId() {
        return id;
    }

    public StorageLevel getStorageLevel() {
        return storageLevel;
    }

    public CallSite getCreationSite() {
        return creationSite;
    }

    private void clearDependencies() {
        dependencies = null;
    }

    private boolean isCheckpointedAndMaterialized() {
        return checkpointData != null && checkpointData.isCheckpointed();
    }
}
