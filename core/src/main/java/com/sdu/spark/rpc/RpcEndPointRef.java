package com.sdu.spark.rpc;

import java.io.Serializable;
import java.util.concurrent.Future;

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
     *
     * */
    public abstract void send(Object message);

    public abstract <T> Future<T> ask(Object message, int timeout);
}
