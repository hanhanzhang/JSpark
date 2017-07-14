package com.sdu.spark.rpc;

import com.sdu.spark.SecurityManager;
import com.sdu.spark.rpc.netty.NettyRpcEnvFactory;

import java.util.concurrent.Future;

/**
 *
 * @author hanhan.zhang
 * */
public interface RpcEnv {

    // RpcEnv Server监听地址
    RpcAddress address();

    // RpcEndPoint节点引用
    RpcEndPointRef endPointRef(RpcEndPoint endPoint);

    // RpcEndPoint节点注册
    RpcEndPointRef setRpcEndPointRef(String name, RpcEndPoint endPoint);
    RpcEndPointRef setRpcEndPointRef(String name, RpcAddress rpcAddress);
    RpcEndPointRef setupEndpointRefByURI(String uri);
    Future<RpcEndPointRef> asyncSetupEndpointRefByURI(String uri);

    // RpcEndPoint关闭
    void stop(RpcEndPoint endPoint);

    // RpcEnv关闭
    void awaitTermination();
    void shutdown();


    /********************************Spark RpcEnv*************************************/
    static RpcEnv create(String host, int port, SparkConf conf, SecurityManager securityManager) {
       return create(host, port, conf, securityManager, false);
    }

    static RpcEnv create(String host, int port, SparkConf conf,
                                SecurityManager securityManager, boolean clientModel) {
        RpcEnvConfig rpcEnvConf = new RpcEnvConfig(conf, host, port, securityManager, clientModel);
        return new NettyRpcEnvFactory().create(rpcEnvConf);
    }
}
