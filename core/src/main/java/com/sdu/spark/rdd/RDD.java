package com.sdu.spark.rdd;

import com.sdu.spark.Dependency;
import com.sdu.spark.Partition;
import com.sdu.spark.SparkContext;
import com.sdu.spark.storage.RDDInfo;
import com.sdu.spark.storage.StorageLevel;
import com.sdu.spark.utils.CallSite;

import java.io.Serializable;
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

    public RDD(SparkContext sc, List<Dependency<?>> deps) {
        this.sc = sc;
        this.id = this.sc.newRddId();
        this.creationSite = this.sc.getCallSite();
        this.deps = deps;
    }


    public final List<Dependency<?>> dependencies() {
        throw new UnsupportedOperationException("");
    }

    public List<Partition> partitions() {
        throw new UnsupportedOperationException("");
    }

    public List<RDD> getNarrowAncestors() {
        throw new UnsupportedOperationException("");
    }
}
