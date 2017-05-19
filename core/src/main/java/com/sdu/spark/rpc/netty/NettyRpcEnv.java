package com.sdu.spark.rpc.netty;

import com.google.common.collect.Maps;
import com.sdu.spark.network.TransportContext;
import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.network.client.TransportClientBootstrap;
import com.sdu.spark.network.client.TransportClientFactory;
import com.sdu.spark.network.server.StreamManager;
import com.sdu.spark.network.utils.TransportConfig;
import com.sdu.spark.rpc.*;
import com.sdu.spark.rpc.netty.OutboxMessage.*;
import com.sdu.spark.rpc.netty.OutboxMessage.CheckExistence;
import com.sdu.spark.rpc.netty.OutboxMessage.OneWayOutboxMessage;
import com.sdu.spark.utils.ThreadUtils;
import com.sdu.spark.utils.Utils;

import java.util.Collections;
import java.util.List;
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
     * 远端服务连接线程
     * */
    private ThreadPoolExecutor clientConnectionExecutor;
    /**
     * 投递消息线程
     * */
    private ThreadPoolExecutor deliverMessageExecutor;
    /**
     * Netty通信上下文
     * */
    private TransportContext transportContext;
    private TransportClientFactory clientFactory;

    private AtomicBoolean stopped = new AtomicBoolean(false);

    public NettyRpcEnv(JSparkConfig sparkConfig) {
        this.host = sparkConfig.getHost();
        this.dispatcher = new Dispatcher(this, sparkConfig);
        this.clientConnectionExecutor = ThreadUtils.newDaemonCachedThreadPool("netty-rpc-connect-%d", sparkConfig.getRpcConnectThreads(), 60);
        this.deliverMessageExecutor = ThreadUtils.newDaemonCachedThreadPool("rpc-deliver-message-%d", sparkConfig.getDeliverThreads(), 60);
        StreamManager streamManager = null;
        this.transportContext = new TransportContext(fromSparkConf(sparkConfig), new NettyRpcHandler(streamManager, this.dispatcher, this));
        this.clientFactory = this.transportContext.createClientFactory(createClientBootstraps());
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

    @Override
    public RpcEndPointRef setRpcEndPointRef(String name, RpcAddress rpcAddress) {
        NettyRpcEndPointRef endPointRef = new NettyRpcEndPointRef(name, rpcAddress, this);
        Future<?> future =  endPointRef.ask(new CheckExistence(name));
        return Utils.getFutureResult(future);
    }

    /**
     * 单向消息[即不需要消息响应]
     * */
    public void send(RequestMessage message) {
        RpcAddress address = message.receiver.address();
        if (address() == address) { // 发送给本地的消息

        } else { // 发送给远端的消息
            postToOutbox(message.receiver, new OneWayOutboxMessage(message.serialize()));
        }
    }

    /**
     * 双向消息[需要消息响应]
     * */
    public Future<?> ask(RequestMessage message) {
        if (message.receiver.address() == address()) {
            // 发送本地消息
            return deliverMessageExecutor.submit(() -> dispatcher.postLocalMessage(message));
        } else {
            // 发送网络消息
            NettyRpcResponseCallback callback = new NettyRpcResponseCallback();
            OutboxMessage.RpcOutboxMessage outboxMessage = new RpcOutboxMessage(message.serialize(), callback);
            postToOutbox(message.receiver, outboxMessage);
//            RpcOutboxMessage outboxMessage = new RpcOutboxMessage(message.serialize());
//            return deliverMessageExecutor.submit(() -> postToOutbox());
            return callback.getResponseFuture();
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
        try {
            return clientFactory.createClient(address.getHost(), address.getPort());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

    }

    public Future<TransportClient> asyncCreateClient(RpcAddress address) {
        return clientConnectionExecutor.submit(() -> createClient(address));
    }

    public void removeOutbox(RpcAddress address) {
        Outbox outbox = outboxes.remove(address);
        if (outbox != null) {
            outbox.stop();
        }
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

    private TransportConfig fromSparkConf(JSparkConfig conf) {
        return null;
    }

    private List<TransportClientBootstrap> createClientBootstraps() {
        return Collections.emptyList();
    }

}
