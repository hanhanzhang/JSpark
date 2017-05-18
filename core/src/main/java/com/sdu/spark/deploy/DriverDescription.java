package com.sdu.spark.deploy;

import lombok.AllArgsConstructor;

import java.io.Serializable;

/**
 * 应用Driver信息
 *
 * @author hanhan.zhang
 * */
@AllArgsConstructor
public class DriverDescription implements Serializable {
    /**
     * 应用Jar包依赖
     * */
    public String jarUrl;
    /**
     * 应用分配内存数
     * */
    public int mem;
    /**
     * 应用分配CPU数
     * */
    public int cores;
    public boolean supervise;
    /**
     * 应用启动命令
     * */
    public Command command;

}
