package com.sdu.spark.deploy;

import com.google.common.collect.Lists;
import com.sdu.spark.rpc.RpcEndPointRef;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

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


    /************************************无需序列化***************************************/
    /**
     * 应用运行状态
     * */
    public transient ApplicationState state;
    /**
     * 应用分配Executor信息
     * */
    public transient HashMap<Integer, ExecutorDesc> executors;
    /**
     * 应用移除的Executor信息
     * */
    private transient List<ExecutorDesc> removedExecutors;
    /**
     * 分配CPU数
     * */
    private transient int coresGranted;

    /**
     * 应用运行结束时间
     * */
    public transient long endTime;
    private transient int nextExecutorId;

    public ApplicationInfo(long startTime, String id, ApplicationDescription desc, Date submitDate, RpcEndPointRef driver, int defaultCores) {
        this.startTime = startTime;
        this.id = id;
        this.desc = desc;
        this.submitDate = submitDate;
        this.driver = driver;
        this.defaultCores = defaultCores;
        this.nextExecutorId = 0;
        this.removedExecutors = Lists.newLinkedList();
    }

    private int requestedCores() {
        return desc.maxCores == 0 ? defaultCores : desc.maxCores;
    }

    public int coreLeft() {
        return requestedCores() - coresGranted;
    }

    private int newExecutorId(){
        return nextExecutorId++;
    }

    public ExecutorDesc addExecutor(WorkerInfo worker, int cores) {
        ExecutorDesc exec = new ExecutorDesc(newExecutorId(), this, worker, cores, desc.memoryPerExecutorMB);
        executors.put(exec.id, exec);
        coresGranted += cores;
        return exec;
    }

    public boolean isFinished() {
        return state != ApplicationState.RUNNING && state != ApplicationState.WAITING;
    }

    public void removeExecutor(ExecutorDesc desc) {
        if (executors.containsKey(desc.id)) {
            removedExecutors.add(desc);
            executors.remove(desc.id);
            coresGranted -= desc.cores;
        }
    }

    public void markFinished(ApplicationState endState) {
        state = endState;
        endTime = System.currentTimeMillis();
    }
}
