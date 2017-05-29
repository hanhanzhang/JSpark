package com.sdu.spark.rpc;

/**
 * @author hanhan.zhang
 * */
public interface RpcEnvFactory {

    RpcEnv create(RpcEnvConfig conf);

}
