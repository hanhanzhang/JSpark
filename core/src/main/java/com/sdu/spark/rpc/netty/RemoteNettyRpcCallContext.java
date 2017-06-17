package com.sdu.spark.rpc.netty;

import com.sdu.spark.network.client.RpcResponseCallback;
import com.sdu.spark.rpc.RpcAddress;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public class RemoteNettyRpcCallContext extends NettyRpcCallContext {

    private NettyRpcEnv rpcEnv;
    private RpcResponseCallback callback;

    public RemoteNettyRpcCallContext(RpcAddress senderAddress, NettyRpcEnv rpcEnv, RpcResponseCallback callback) {
        super(senderAddress);
        this.rpcEnv = rpcEnv;
        this.callback = callback;
    }

    @Override
    public void send(Object message) {
        try {
            ByteBuffer response = rpcEnv.serialize(message);
            callback.onSuccess(response);
        } catch (IOException e) {
            callback.onFailure(e);
        }
    }
}
