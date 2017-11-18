package com.sdu.spark.rpc.netty;

import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.rpc.RpcAddress;
import com.sdu.spark.rpc.RpcEndpointRef;
import com.sdu.spark.rpc.RpcEndpointAddress;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.*;

import static com.sdu.spark.utils.RpcUtils.getRpcAskTimeout;

/**
 * Note:
 *
 *  1: {@link NettyRpcEndpointRef}归属于{@link NettyRpcEnv}
 *
 *  2: 使用{@link NettyRpcEndpointRef}发送消息时:
 *
 *      1': 发送方地址 = RpcEnv.address[即RpcEnv启动的RpcServer地址]
 *
 *      2': 接收方地址 = {@link NettyRpcEndpointRef#address}
 *
 *  todo:
 *      {@link #readObject(ObjectInputStream)}
 *      {@link #writeObject(ObjectOutputStream)}
 *
 * @author hanhan.zhang
 * */
public class NettyRpcEndpointRef extends RpcEndpointRef {

    // 引用RpcEndPoint节点的网络地址
    private RpcEndpointAddress endpointAddress;
    // RpcEndPoint节点客户端
    public transient volatile TransportClient client;

    public transient volatile NettyRpcEnv nettyEnv;

    private final long defaultAskTimeout;

    public NettyRpcEndpointRef(RpcEndpointAddress address, NettyRpcEnv nettyEnv) {
        this.endpointAddress = address;
        this.nettyEnv = nettyEnv;

        this.defaultAskTimeout = getRpcAskTimeout(nettyEnv.conf);
    }



    @Override
    public String name() {
        return endpointAddress == null ? null : endpointAddress.name;
    }

    @Override
    public RpcAddress address() {
        return endpointAddress == null ? null : endpointAddress.address;
    }

    @Override
    public void send(Object message) {
        assert nettyEnv != null;
        nettyEnv.send(new RequestMessage(nettyEnv.address(), this, message));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> CompletableFuture<T> ask(Object message) {
        assert nettyEnv != null;
        return (CompletableFuture<T>) nettyEnv.ask(new RequestMessage(nettyEnv.address(), this, message));
    }

    @Override
    public Object askSync(Object message) throws TimeoutException, InterruptedException, ExecutionException {
        return askSync(message, defaultAskTimeout);
    }

    @Override
    public Object askSync(Object message, long timeout) throws TimeoutException, InterruptedException, ExecutionException {
        return ask(message).get(timeout, TimeUnit.MILLISECONDS);
    }

    /***************************************自定义序列化***************************************/
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.nettyEnv = NettyRpcEnv.currentEnv;
        this.client = NettyRpcEnv.currentClient;
    }
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
    }


    @Override
    public String toString() {
        return String.format("NettyRpcEndpointRef(%s)", endpointAddress);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NettyRpcEndpointRef that = (NettyRpcEndpointRef) o;

        return that.endpointAddress.equals(this.endpointAddress);
    }

    @Override
    public int hashCode() {
        return endpointAddress == null ? 0 : endpointAddress.hashCode();
    }
}
