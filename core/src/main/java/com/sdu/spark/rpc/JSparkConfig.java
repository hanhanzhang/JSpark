package com.sdu.spark.rpc;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
@Getter
@Builder
public class JSparkConfig implements Serializable {

    /**
     * Rpc事件分发线程数
     * */
    private int dispatcherThreads;
    /**
     * Rpc连接线程数
     * */
    private int rpcConnectThreads;
    /**
     * Rpc消息投递线程数
     * */
    private int deliverThreads;

    /**
     * Master定时check工作节点时间间隔
     * */
    private int checkWorkerTimeout;
    /**
     * Worker节点心跳超时时间
     * */
    private int workerTimeout;
    /**
     * 挂掉Worker持续存在的最大心跳次数
     * */
    private int deadWorkerPersistenceTimes;
    /**
     * 向Master注册重试次数
     * */
    private int maxRetryConnectTimes;
    /**
     * RpcServer是否验证
     * */
    private boolean authenticationEnabled;
}
