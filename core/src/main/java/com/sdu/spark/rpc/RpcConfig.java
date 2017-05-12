package com.sdu.spark.rpc;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
@Getter
@Builder
public class RpcConfig implements Serializable {

    /**
     * RpcEnv Server绑定的地址
     * */
    private String host;
    /**
     * Rpc事件分发线程数
     * */
    private int dispatcherThreads;
    /**
     * Rpc连接线程数
     * */
    private int rpcConnectThreads;

    /**
     * Master检测工作节点超时时间
     * */
    private int checkWorkerTimeout;
}
