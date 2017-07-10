package com.sdu.spark.rpc.netty;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sdu.spark.SecurityManager;
import com.sdu.spark.network.TransportContext;
import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.network.client.TransportClientBootstrap;
import com.sdu.spark.network.client.TransportClientFactory;
import com.sdu.spark.network.crypto.AuthServerBootstrap;
import com.sdu.spark.network.server.StreamManager;
import com.sdu.spark.network.server.TransportServer;
import com.sdu.spark.network.server.TransportServerBootstrap;
import com.sdu.spark.network.utils.IOModel;
import com.sdu.spark.network.utils.TransportConf;
import com.sdu.spark.rpc.*;
import com.sdu.spark.rpc.netty.OutboxMessage.CheckExistence;
import com.sdu.spark.rpc.netty.OutboxMessage.OneWayOutboxMessage;
import com.sdu.spark.rpc.netty.OutboxMessage.RpcOutboxMessage;
import com.sdu.spark.serializer.JavaSerializerInstance;
import com.sdu.spark.serializer.SerializationStream;
import com.sdu.spark.utils.ThreadUtils;
import com.sdu.spark.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyRpcEnv.class);

    private String host;

    private SparkConf conf;
    /**
     * Spark权限管理模块
     * */
    private SecurityManager securityManager;
    /**
     * Spark网络序列化实例
     * */
    private JavaSerializerInstance serializerInstance;
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

    public NettyRpcEnv(SparkConf sparkConfig, String host, JavaSerializerInstance serializerInstance, SecurityManager securityManager) {
        this.conf = sparkConfig;
        this.host = host;
        this.dispatcher = new Dispatcher(this, sparkConfig);
        this.clientConnectionExecutor = ThreadUtils.newDaemonCachedThreadPool("netty-rpc-connect-%d", sparkConfig.getRpcConnectThreads(), 60);
        this.deliverMessageExecutor = ThreadUtils.newDaemonCachedThreadPool("rpc-deliver-message-%d", sparkConfig.getDeliverThreads(), 60);
        StreamManager streamManager = null;
        this.transportContext = new TransportContext(fromSparkConf(sparkConfig), new NettyRpcHandler(streamManager, this.dispatcher, this));
        this.clientFactory = this.transportContext.createClientFactory(createClientBootstraps());
        this.serializerInstance = serializerInstance;
        this.securityManager = securityManager;
    }

    @Override
    public RpcAddress address() {
        return server != null ? new RpcAddress(host, server.getPort()) : null;
    }


    /****************************RpcEndPoint节点注册****************************/
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
        NettyRpcEndPointRef rpcEndPointRef = null;
        try {
            NettyRpcEndPointRef verifier = new NettyRpcEndPointRef(RpcEndpointVerifier.NAME, rpcAddress, this);
            Future<?> future =  verifier.ask(new CheckExistence(name));
            rpcEndPointRef = Utils.getFutureResult(future);
            rpcEndPointRef.setRpcEnv(this);
            return rpcEndPointRef;
        } finally {
            if (rpcEndPointRef != null) {
                LOGGER.info("RpcEnv注册远端RpcEndPoint的引用RpcEndPointRef(host = {}, name = {})",
                        rpcEndPointRef.address().hostPort(), rpcEndPointRef.name());
            }
        }
    }


    /*******************************Rpc消息发送***********************************/
    /**
     * 单向消息[即不需要消息响应]
     * */
    public void send(RequestMessage message) {
        RpcAddress address = message.receiver.address();
        if (address().equals(address)) {
            // 发送给本地的消息
            dispatcher.postOneWayMessage(message);
        } else {
            // 发送给远端的消息
            postToOutbox(message.receiver, new OneWayOutboxMessage(message.serialize(this)));
        }
    }

    /**
     * 双向消息[需要消息响应]
     * */
    public Future<?> ask(RequestMessage message) {
        if (message.receiver.address().equals(address())) {
            // 发送本地消息
            return deliverMessageExecutor.submit(() -> dispatcher.postLocalMessage(message));
        } else {
            // 发送网络消息
            NettyRpcResponseCallback callback = new NettyRpcResponseCallback(this);
            OutboxMessage.RpcOutboxMessage outboxMessage = new RpcOutboxMessage(message.serialize(this), callback);
            postToOutbox(message.receiver, outboxMessage);
            return callback.getResponseFuture();
        }
    }

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

    public void removeOutbox(RpcAddress address) {
        Outbox outbox = outboxes.remove(address);
        if (outbox != null) {
            outbox.stop();
        }
    }

    /*****************************RpcEnv关联RpcServer启动********************************/
    public void startServer(String host, int port) {
        List<TransportServerBootstrap> bootstraps;
        if (securityManager.isAuthenticationEnabled()) {
            bootstraps = Lists.newArrayList(new AuthServerBootstrap(fromSparkConf(conf), securityManager));
        } else {
            bootstraps = Collections.emptyList();
        }
        server = transportContext.createServer(host, port, bootstraps);
        // 注册RpcEndPoint节点
        dispatcher.registerRpcEndPoint(RpcEndpointVerifier.NAME, new RpcEndpointVerifier(this, dispatcher));
    }

    public TransportClient createClient(RpcAddress address) {
        try {
            return clientFactory.createClient(address.host, address.port);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

    }

    public Future<TransportClient> asyncCreateClient(RpcAddress address) {
        return clientConnectionExecutor.submit(() -> createClient(address));
    }

    /*****************************Rpc网络消息序列化*****************************/
    public ByteBuffer serialize(Object content) throws IOException {
        return serializerInstance.serialize(content);
    }

    public SerializationStream serializeStream(OutputStream out) throws IOException {
        return serializerInstance.serializeStream(out);
    }

    public <T> T deserialize(TransportClient client, ByteBuffer buf) throws IOException {
        return serializerInstance.deserialize(buf);
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

    private TransportConf fromSparkConf(SparkConf conf) {
        return new TransportConf(IOModel.NIO.name(), Collections.emptyMap());
    }

    private List<TransportClientBootstrap> createClientBootstraps() {
        return Collections.emptyList();
    }

}
