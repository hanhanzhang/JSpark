package com.sdu.spark.storage;

import com.sdu.spark.rdd.RDD;
import com.sdu.spark.rdd.RDDOperationScope;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
public class RDDInfo implements Comparable<RDDInfo>{
    public int id;
    public String name;
    public int numPartitions;
    public StorageLevel storageLevel;
    public List<Integer> parentIds;
    public String callSite;
    public RDDOperationScope scope;

    public int numCachedPartitions = 0;
    public long memSize = 0L;
    public long diskSize = 0L;
    public long externalBlockStoreSize = 0L;

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
        throw new UnsupportedOperationException("");
    }

    @Override
    public int compareTo(RDDInfo o) {
        return this.id - o.id;
    }
}
