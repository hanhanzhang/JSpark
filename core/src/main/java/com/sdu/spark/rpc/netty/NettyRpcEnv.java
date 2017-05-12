package com.sdu.spark.rpc.netty;

import com.google.common.collect.Maps;
import com.sdu.spark.rpc.*;
import com.sdu.spark.rpc.netty.OutboxMessage.*;
import com.sdu.spark.utils.ThreadUtils;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author hanhan.zhang
 * */
public class NettyRpcEnv extends RpcEnv {
    private String host;
    /**
     * RpcEnv Server端, 负责网络数据传输
     * */
    private TransportServer server;
    /**
     * 消息路由分发
     * */
    private Dispatcher dispatcher;
    /**
     * 消息发送信箱[key = 待接收地址, value = 发送信箱]
     * */
    private Map<RpcAddress, Outbox> outboxes = Maps.newConcurrentMap();
    /**
     * 远端服务客户端工厂
     * */
    private TransportClientFactory clientFactory = new TransportClientFactory();
    /**
     * 远端服务连接线程
     * */
    private ThreadPoolExecutor clientConnectionExecutor;

    private AtomicBoolean stopped = new AtomicBoolean(false);

    public NettyRpcEnv(RpcConfig rpcConfig) {
        this.host = rpcConfig.getHost();
        this.dispatcher = new Dispatcher(this, rpcConfig);
        this.clientConnectionExecutor = ThreadUtils.newDaemonCachedThreadPool("netty-rpc-connect-%d", rpcConfig.getRpcConnectThreads(), 60);
    }

    @Override
    public RpcAddress address() {
        return server != null ? new RpcAddress(host, server.getPort()) : null;
    }

    @Override
    public RpcEndPointRef endPointRef(RpcEndPoint endPoint) {
        return dispatcher.getRpcEndPointRef(endPoint);
    }

    @Override
    public RpcEndPointRef setRpcEndPointRef(String name, RpcEndPoint endPoint) {
        return dispatcher.registerRpcEndPoint(name, endPoint);
    }


    /**
     * 发送消息
     * */
    public void send(RequestMessage message) {
        RpcAddress address = message.getReceiver().address();
        if (address() == address) { // 发送给本地的消息

        } else { // 发送给远端的消息
            postToOutbox(message.getReceiver(), new OneWayOutboxMessage(message.serialize()));
        }
    }

    /**
     *
     * */
    private void postToOutbox(NettyRpcEndPointRef receiver, OutboxMessage message) {
        if (receiver.getClient() != null) {
            message.sendWith(receiver.getClient());
        } else {
            if (receiver.address() == null) {
                throw new IllegalStateException("Cannot send message to client endpoint with no listen address.");
            }
            Outbox outbox = outboxes.get(receiver.address());
            if (outbox == null) {
                Outbox newOutbox = new Outbox(this, receiver.address());
                Outbox oldOutbox = outboxes.putIfAbsent(receiver.address(), newOutbox);
                if (oldOutbox == null) {
                    outbox = newOutbox;
                } else {
                    outbox = oldOutbox;
                }
            }
            if (stopped.get()) {
                outboxes.remove(receiver.address());
                outbox.stop();
            } else {
                outbox.send(message);
            }
        }
    }

    public TransportClient createClient(RpcAddress address) {
        return clientFactory.createClient(address.getHost(), address.getPort());
    }

    public Future<TransportClient> asyncCreateClient(RpcAddress address) {
        return clientConnectionExecutor.submit(() -> createClient(address));
    }

    @Override
    public void awaitTermination() {
        dispatcher.awaitTermination();
    }

    @Override
    public void stop(RpcEndPoint endPoint) {

    }

    @Override
    public void shutdown() {

    }

}
