package com.sdu.spark.deploy;

import java.io.Serializable;

/**
 * 应用分配的JExecutor资源信息
 *
 * @author hanhan.zhang
 * */
public class ExecutorDesc implements Serializable {
    // 唯一标识
    public int id;
    // 所属Spark应用
    public ApplicationInfo application;
    // 所属Worker
    public WorkerInfo worker;
    public int cores;
    public int memory;

    public ExecutorState state = ExecutorState.LAUNCHING;

    public ExecutorDesc(int id, ApplicationInfo application, WorkerInfo worker, int cores, int memory) {
        this.id = id;
        this.application = application;
        this.worker = worker;
        this.cores = cores;
        this.memory = memory;
    }

    public String fullId() {
      return application.id + "/" + id;
    }

}
