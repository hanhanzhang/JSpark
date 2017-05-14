package com.sdu.spark.rpc.netty;

import com.sdu.spark.rpc.RpcAddress;
import com.sdu.spark.rpc.RpcEndPointRef;
import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.Future;

/**
 * @author hanhan.zhang
 * */
public class NettyRpcEndPointRef extends RpcEndPointRef {

    private String name;

    /**
     * 被引用Rpc节点的地址
     * */
    private RpcAddress address;

    private NettyRpcEnv rpcEnv;

    @Getter
    @Setter
    private TransportClient client;

    public NettyRpcEndPointRef(String name, RpcAddress address, NettyRpcEnv rpcEnv) {
        this.name = name;
        this.address = address;
        this.rpcEnv = rpcEnv;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public RpcAddress address() {
        return address;
    }

    @Override
    public void send(Object message) {
        assert rpcEnv != null;
        rpcEnv.send(new RequestMessage(address(), this, message));
    }

    @Override
    public Future<?> ask(Object message) {
        assert rpcEnv != null;
        return rpcEnv.ask(new RequestMessage(address(), this, message));
    }

    @Override
    public <T> Future<T> ask(Object message, int timeout) {
        return null;
    }
}
