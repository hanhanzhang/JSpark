package com.sdu.spark.deploy;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

/**
 * JSpark Executor信息
 *
 * @author hanhan.zhang
 * */
@Getter
@AllArgsConstructor
public class ExecutorDescription implements Serializable {
    /**
     * Executor上部署的应用标识
     * */
    private String appId;
    /**
     * Executor标识
     * */
    private String executorId;
    /**
     * Executor使用CPU数量
     * */
    private int cores;
    /**
     * Executor使用内存
     * */
    private int memory;
    /**
     * Executor状态
     * */
    private ExecutorState state;

    @Override
    public String toString() {
        return "ExecutorDescription(" +
                "appId='" + appId + '\'' +
                ", executorId='" + executorId + '\'' +
                ", cores=" + cores +
                ", memory=" + memory +
                ", state=" + state +
                ')';
    }
}
