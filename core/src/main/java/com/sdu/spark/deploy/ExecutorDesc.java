package com.sdu.spark.deploy;

import lombok.AllArgsConstructor;

import java.io.Serializable;

/**
 * 应用分配的JExecutor资源信息
 *
 * @author hanhan.zhang
 * */
@AllArgsConstructor
public class ExecutorDesc implements Serializable {
    public int id;
    /**
     * 应用信息
     * */
    public ApplicationInfo application;
    /**
     * 应用分配Worker信息
     * */
    public WorkerInfo worker;
    public int cores;
    public int memory;


    public String fullId() {
      return application.id + "/" + id;
    }

}
