package com.sdu.spark.rpc.netty;

import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.rpc.RpcAddress;
import com.sdu.spark.rpc.RpcEndPointRef;
import com.sdu.spark.rpc.RpcEndpointAddress;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.utils.RpcUtils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.sdu.spark.utils.RpcUtils.getRpcAskTimeout;

/**
 * Note:
 *
 *  1: {@link NettyRpcEndPointRef}归属于{@link NettyRpcEnv}
 *
 *  2: 使用{@link NettyRpcEndPointRef}发送消息时:
 *
 *      1': 发送方地址 = RpcEnv.address[即RpcEnv启动的RpcServer地址]
 *
 *      2': 接收方地址 = {@link NettyRpcEndPointRef#address}
 *
 *  todo:
 *      {@link #readObject(ObjectInputStream)}
 *      {@link #writeObject(ObjectOutputStream)}
 *
 * @author hanhan.zhang
 * */
public class NettyRpcEndPointRef extends RpcEndPointRef {

    // 引用RpcEndPoint节点的网络地址
    private RpcEndpointAddress endpointAddress;
    // RpcEndPoint节点客户端
    public transient volatile TransportClient client;

    public transient volatile NettyRpcEnv rpcEnv;

    private final long defaultAskTimeout;

    public NettyRpcEndPointRef(RpcEndpointAddress address, NettyRpcEnv rpcEnv) {
        this.endpointAddress = address;
        this.rpcEnv = rpcEnv;

        this.defaultAskTimeout = getRpcAskTimeout(rpcEnv.conf);
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
        assert rpcEnv != null;
        rpcEnv.send(new RequestMessage(rpcEnv.address(), this, message));
    }

    @Override
    public <T> Future<T> ask(Object message) {
        assert rpcEnv != null;
        return (Future<T>) rpcEnv.ask(new RequestMessage(rpcEnv.address(), this, message));
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
        this.rpcEnv = NettyRpcEnv.currentEnv;
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

        NettyRpcEndPointRef that = (NettyRpcEndPointRef) o;

        return that.endpointAddress.equals(this.endpointAddress);
    }

    @Override
    public int hashCode() {
        return endpointAddress == null ? 0 : endpointAddress.hashCode();
    }
}
