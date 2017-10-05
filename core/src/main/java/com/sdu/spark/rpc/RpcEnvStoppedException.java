package com.sdu.spark.rpc;

/**
 * @author hanhan.zhang
 * */
public class RpcEnvStoppedException extends IllegalStateException {
    public RpcEnvStoppedException() {
        super("RpcEnv already stopped.");
    }
}
