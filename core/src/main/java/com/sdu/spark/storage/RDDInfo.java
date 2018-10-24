package com.sdu.spark.storage;

import com.google.common.collect.Lists;
import com.sdu.spark.Dependency;
import com.sdu.spark.rdd.RDD;
import com.sdu.spark.rdd.RDDOperationScope;
import com.sdu.spark.utils.Utils;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
public class RDDInfo implements Comparable<RDDInfo>{

    /** RDD的id */
    private int id;
    /** RDD的名称 */
    private String name;
    /** RDD的分区数量 */
    private int numPartitions;
    /** RDD的存储级别 */
    private StorageLevel storageLevel;
    /** RDD的父RDD的id集合 */
    private List<Integer> parentIds;
    /** RDD用户调用栈信息 */
    private String callSite;
    /** RDD操作范围 */
    private RDDOperationScope scope;
    /** RDD缓存的分区数量 */
    private int numCachedPartitions = 0;
    /** RDD使用内存大小 */
    private long memSize = 0L;
    /** RDD使用磁盘大小 */
    private long diskSize = 0L;
    /** Block存储在外部大小 */
    private long externalBlockStoreSize = 0L;

    public RDDInfo(int id, String name, int numPartitions, StorageLevel storageLevel,
                   List<Integer> parentIds, String callSite) {
        this(id, name, numPartitions, storageLevel, parentIds, callSite, null);
    }

    public RDDInfo(int id, String name, int numPartitions, StorageLevel storageLevel,
                   List<Integer> parentIds, String callSite, RDDOperationScope scope) {
        this.id = id;
        this.name = name;
        this.numPartitions = numPartitions;
        this.storageLevel = storageLevel;
        this.parentIds = parentIds;
        this.callSite = callSite;
        this.scope = scope;
    }



    public boolean isCached() {
        return (memSize + diskSize > 0) && numCachedPartitions > 0;
    }

    public static RDDInfo fromRDD(RDD<?> rdd) {
        String rddName = rdd.getName();
        if (rddName == null || rddName.isEmpty()) {
            rddName = Utils.getFormattedClassName(rdd);
        }
        List<Integer> parentIds = Lists.newArrayList();
        for (Dependency<?> dependency : rdd.dependencies()) {
            parentIds.add(dependency.rdd().getId());
        }
        return new RDDInfo(rdd.getId(),
                           rddName,
                           rdd.partitions().length,
                           rdd.getStorageLevel(),
                           parentIds,
                           rdd.getCreationSite().shortForm);
    }

    @Override
    public int compareTo(RDDInfo o) {
        return this.id - o.id;
    }
}
