package com.sdu.spark.rpc;

import com.sdu.spark.utils.RpcUtils;

import java.io.Serializable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 *
 *
 * @author hanhan.zhang
 * */
public abstract class RpcEndpointRef implements Serializable {

    protected int maxRetries;
    protected long retryWaitMs;
    protected long defaultAskTimeout;


    public RpcEndpointRef(SparkConf conf) {
        maxRetries = RpcUtils.numRetries(conf);
        retryWaitMs = RpcUtils.retryWaitMs(conf);
        defaultAskTimeout = RpcUtils.getRpcAskTimeout(conf);
    }

    // 引用RpcEndPoint节点的名称
    public abstract String name();
    // 引用RpcEndPoint节点的网络地址
    public abstract RpcAddress address();

    /*****************************Point-To-Point数据通信*****************************/
    // 发送单向消息[即不需要消息响应]
    public abstract void send(Object message);

    // 发送双向消息[需要消息响应]
    public abstract <T> CompletableFuture<T> ask(Object message);
    public abstract Object askSync(Object message) throws TimeoutException, InterruptedException, ExecutionException;
    public abstract Object askSync(Object message, long timeout) throws TimeoutException, InterruptedException, ExecutionException;
}
