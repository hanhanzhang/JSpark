package com.sdu.spark.rpc;

/**
 *
 * @author hanhan.zhang
 * */
public abstract class RpcEnv {

    /**
     * 返回RpcEnv监听的网络地址
     * */
    public abstract RpcAddress address();

    /**
     * 返回Rpc节点的引用节点
     * */
    public abstract RpcEndPointRef endPointRef(RpcEndPoint endPoint);

    /**
     * RpcEnv注册以Rpc节点
     *
     * @param name : Rpc节点名
     * @param endPoint : Rpc节点
     * */
    public abstract RpcEndPointRef setRpcEndPointRef(String name, RpcEndPoint endPoint);

    /**
     * 关闭Rpc节点
     * */
    public abstract void stop(RpcEndPoint endPoint);

    public abstract void awaitTermination();

    /**
     * 关闭RpcEnv
     * */
    public abstract void shutdown();
}
