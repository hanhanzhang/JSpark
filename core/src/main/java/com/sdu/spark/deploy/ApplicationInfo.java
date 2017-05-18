package com.sdu.spark.deploy;

import com.google.common.collect.HashBiMap;
import com.sdu.spark.rpc.RpcEndPointRef;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;

/**
 * 应用信息
 *
 * @author hanhan.zhang
 * */
public class ApplicationInfo implements Serializable {
    /**
     * 应用启动时间
     * */
    public long startTime;
    /**
     * 应用标识
     * */
    public String id;
    /**
     * 应用运行资源分配及启动信息
     * */
    public ApplicationDescription desc;
    /**
     * 应用提交信息
     * */
    public Date submitDate;
    /**
     * 应用启动节点
     * */
    public RpcEndPointRef driver;
    /**
     * 默认分配CPU数
     * */
    public int defaultCores;
    /**
     * 应用运行状态
     * */
    @Setter
    @Getter
    private ApplicationState state;
    /**
     * 应用分配Executor信息
     * */
    @Setter
    @Getter
    private HashMap<Integer, ExecutorDesc> executors;
    /**
     * 应用移除的Executor信息
     * */
    @Setter
    @Getter
    private ExecutorDesc[] removedExecutors;
    /**
     * 分配CPU数
     * */
    @Setter
    @Getter
    private int coresGranted;
    /**
     * 应用运行结束时间
     * */
    @Setter
    @Getter
    private long endTime;

    public ApplicationInfo(long startTime, String id, ApplicationDescription desc, Date submitDate, RpcEndPointRef driver, int defaultCores) {
        this.startTime = startTime;
        this.id = id;
        this.desc = desc;
        this.submitDate = submitDate;
        this.driver = driver;
        this.defaultCores = defaultCores;
    }
}
