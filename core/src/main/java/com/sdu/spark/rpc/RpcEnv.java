package com.sdu.spark.rpc;

import com.sdu.spark.SecurityManager;
import com.sdu.spark.rpc.netty.NettyRpcEnvFactory;

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
    public abstract RpcEndPointRef setRpcEndPointRef(String name, RpcAddress rpcAddress);
    public abstract RpcEndPointRef setupEndpointRefByURI(String uri);

    /**
     * 关闭Rpc节点
     * */
    public abstract void stop(RpcEndPoint endPoint);

    public abstract void awaitTermination();

    /**
     * 关闭RpcEnv
     * */
    public abstract void shutdown();

    /**
     * 创建RpcEnv
     * */
    public static RpcEnv create(String host, int port, SparkConf conf, SecurityManager securityManager) {
       return create(host, port, conf, securityManager, false);
    }

    public static RpcEnv create(String host, int port, SparkConf conf,
                                SecurityManager securityManager, boolean clientModel) {
        RpcEnvConfig rpcEnvConf = new RpcEnvConfig(conf, host, port, securityManager, clientModel);
        return new NettyRpcEnvFactory().create(rpcEnvConf);
    }
}
