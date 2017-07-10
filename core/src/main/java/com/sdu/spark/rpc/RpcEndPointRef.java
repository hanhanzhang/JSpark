package com.sdu.spark.rpc;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

/**
 *
 *
 * @author hanhan.zhang
 * */
public abstract class RpcEndPointRef implements Serializable {

    public abstract String name();

    /**
     * 被引用Rpc节点的地址
     * */
    public abstract RpcAddress address();

    /**
     * 发送单向消息[即不需要消息响应]
     * */
    public abstract void send(Object message);

    /**
     * 发送双向消息[需要消息响应]
     * */
    public abstract Future<?> ask(Object message);

    public abstract Object askSync(Object message, long timeout) throws TimeoutException, InterruptedException, ExecutionException;
}
