package com.sdu.spark.deploy;

import lombok.AllArgsConstructor;

import java.io.Serializable;

/**
 * 应用运行资源参数
 *
 * @author hanhan.zhang
 * */
@AllArgsConstructor
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
     * 每个JVM进程分配的内存
     * */
    public int memoryPerExecutorMB;
    /**
     * 应用启动命令
     * */
    public Command command;

}
