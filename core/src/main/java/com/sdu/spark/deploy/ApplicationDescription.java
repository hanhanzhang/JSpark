package com.sdu.spark.deploy;

import java.io.Serializable;

/**
 * 应用运行资源参数
 *
 * @author hanhan.zhang
 * */
public class ApplicationDescription implements Serializable {
    /**
     * 应用名称
     * */
    public String name;
    /**
     * 分配应用最大CPU数
     * */
    public int maxCores;
    /**
     * 每个Executor分配CPU数
     * */
    public int coresPerExecutor = -1;
    /**
     * 每个JVM进程分配的内存
     * */
    public int memoryPerExecutorMB;
    /**
     * 应用启动命令(即Driver启动程序入口)
     * */
    public Command command;

    public ApplicationDescription(String name, int maxCores, int coresPerExecutor, int memoryPerExecutorMB, Command command) {
        this.name = name;
        this.maxCores = maxCores;
        this.coresPerExecutor = coresPerExecutor;
        this.memoryPerExecutorMB = memoryPerExecutorMB;
        this.command = command;
    }
}
