package com.sdu.spark.rpc.netty;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.SettableFuture;
import com.sdu.spark.SecurityManager;
import com.sdu.spark.SparkException;
import com.sdu.spark.network.TransportContext;
import com.sdu.spark.network.client.RpcResponseCallback;
import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.network.client.TransportClientBootstrap;
import com.sdu.spark.network.client.TransportClientFactory;
import com.sdu.spark.network.crypto.AuthClientBootstrap;
import com.sdu.spark.network.crypto.AuthServerBootstrap;
import com.sdu.spark.network.netty.SparkTransportConf;
import com.sdu.spark.network.server.TransportServer;
import com.sdu.spark.network.server.TransportServerBootstrap;
import com.sdu.spark.network.utils.TransportConf;
import com.sdu.spark.rpc.*;
import com.sdu.spark.rpc.netty.OutboxMessage.CheckExistence;
import com.sdu.spark.rpc.netty.OutboxMessage.OneWayOutboxMessage;
import com.sdu.spark.rpc.netty.OutboxMessage.RpcOutboxMessage;
import com.sdu.spark.serializer.JavaSerializerInstance;
import com.sdu.spark.serializer.SerializationStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.sdu.spark.utils.ThreadUtils.newDaemonCachedThreadPool;

/**
 * {@link NettyRpcEnv}职责：
 *
 * 1: RpcEndpoint(RpcEndPoint负责处理不同RpcMessage)管理
 *
 *    1': 注册RpcEndpoint(具体管理由{@link Dispatcher}实现)
 *
 *    2': 查询RpcEndpoint({@link Dispatcher#verify(String)})
 *
 * 2: Rpc消息路由({@link #send(RequestMessage)}、{@link #ask(RequestMessage)})
 *
 *    1': 若是RpcMessage消息接收地址与RpcEnv地址相同, 则由{@link Dispatcher}向接收信箱投递RpcMessage
 *
 *    2': 若是RpcMessage消息接收地址与RpcEnv地址不同, 则向{@link Outbox}发送信箱投递RpcMessage
 *
 * 3: Network Server/Client
 *
 *   1': {@link TransportContext#createServer(String, int, List)}启动Network Server负责接收RpcMessage
 *
 *   2': {@link TransportClientFactory#createClient(String, int)}创建Remote Server连接并发送RpcMessage
 *
 * @author hanhan.zhang
 * */
public class NettyRpcEnv extends RpcEnv {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyRpcEnv.class);

    public static volatile NettyRpcEnv currentEnv;
    public static volatile TransportClient currentClient;

    private String host;

    /**********************************Spark RpcEnv消息路由************************************/
    /**
     * A map for {@link RpcAddress} and {@link Outbox}. When we are connecting to a remote RpcAddress,
     * we just put messages to its {@link Outbox} to implement a non-blocking `send` method.
     */
    private Map<RpcAddress, Outbox> outboxes = Maps.newConcurrentMap();
    // 接收消息路由
    private Dispatcher dispatcher;
    private NettyStreamManager streamManager;
    // 消息分发线程
    private ThreadPoolExecutor deliverMessageExecutor;


    /**********************************Spark RpcEnv数据传输************************************/
    // Spark权限管理模块
    private SecurityManager securityManager;
    // 数据序列化
    private JavaSerializerInstance javaSerializerInstance;
    // RpcEnv网络数据监听
    private TransportServer server;
    private TransportConf transportConf;
    // 网络数据通信
    private TransportContext transportContext;
    // 客户端连接
    private TransportClientFactory clientFactory;
    public ThreadPoolExecutor clientConnectionExecutor;


    private AtomicBoolean stopped = new AtomicBoolean(false);

    public NettyRpcEnv(SparkConf conf,
                       String host,
                       JavaSerializerInstance serializerInstance,
                       SecurityManager securityManager,
                       int numUsableCores) {
        super(conf);
        this.host = host;
        this.securityManager = securityManager;
        this.javaSerializerInstance = serializerInstance;
        this.dispatcher = new Dispatcher(this, numUsableCores);
        this.streamManager = new NettyStreamManager(this);
        this.clientConnectionExecutor = newDaemonCachedThreadPool("netty-rpc-connect-%d", conf.getInt("spark.rpc.connect.threads", 64), 60);
        this.deliverMessageExecutor = newDaemonCachedThreadPool("rpc-deliver-message-%d", conf.getInt("spark.rpc.deliver.message.threads", 64), 60);
        this.transportConf = SparkTransportConf.fromSparkConf(conf, "rpc", conf.getInt("spark.rpc.io.threads", 0));
        this.transportContext = new TransportContext(transportConf, new NettyRpcHandler(streamManager, this.dispatcher, this));
        this.clientFactory = this.transportContext.createClientFactory(createClientBootstraps());
    }

    @Override
    public RpcAddress address() {
        return server != null ? new RpcAddress(host, server.getPort()) : null;
    }

    /***********************************Spark Point-To-Point管理***************************************/
    @Override
    public RpcEndpointRef endPointRef(RpcEndpoint endPoint) {
        return dispatcher.getRpcEndPointRef(endPoint);
    }

    @Override
    public RpcEndpointRef setRpcEndPointRef(String name, RpcEndpoint endPoint) {
        return dispatcher.registerRpcEndPoint(name, endPoint);
    }

    @Override
    public Future<RpcEndpointRef> asyncSetupEndpointRefByURI(String uri) {
        RpcEndpointAddress endpointAddress = RpcEndpointAddress.apply(uri);
        /**RpcEnv会创建{@link RpcEndpointVerifier}负责EndPointName查询*/
        RpcEndpointAddress verifierEndpointAddress = new RpcEndpointAddress(RpcEndpointVerifier.NAME,
                                                                            endpointAddress.address);
        /**向{@link RpcEndpointVerifier}询问endpointAddress是否存在*/
        NettyRpcEndpointRef verifier = new NettyRpcEndpointRef(verifierEndpointAddress, this);
        return verifier.ask(new CheckExistence(endpointAddress.name));
    }


    /*******************************Spark RpcEnv网络数据传输***********************************/
    // 单向消息
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

    // 双向消息
    public CompletableFuture<?> ask(RequestMessage message) {
        if (message.receiver.address().equals(address())) {
            // 发送本地消息
            return CompletableFuture.runAsync(() -> dispatcher.postLocalMessage(message),
                                              deliverMessageExecutor);
        } else {
            // 发送网络消息
            CompletableFuture<Object> p = new CompletableFuture<>();
            RpcResponseCallback callback = new RpcResponseCallback() {
                @Override
                public void onSuccess(ByteBuffer response) {
                    try {
                        Object result = deserialize(null, response);
                        p.complete(result);
                    } catch (IOException e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    p.completeExceptionally(e);
                }
            };
            OutboxMessage.RpcOutboxMessage outboxMessage = new RpcOutboxMessage(message.serialize(this), callback);
            postToOutbox(message.receiver, outboxMessage);
            return p;
        }
    }

    private void postToOutbox(NettyRpcEndpointRef receiver, OutboxMessage message) {
        if (receiver.client != null) {
            message.sendWith(receiver.client);
        } else {
            assert receiver.address() != null :
                    "Cannot send message to client endpoint with no listen address.";
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

    /*********************************Spark RpcEnv Server************************************/
    public void startServer(String host, int port) {
        List<TransportServerBootstrap> bootstraps;
        if (securityManager.isAuthenticationEnabled()) {
            bootstraps = Lists.newArrayList(new AuthServerBootstrap(transportConf, securityManager));
        } else {
            bootstraps = Collections.emptyList();
        }
        server = transportContext.createServer(host, port, bootstraps);
        /**注册{@link RpcEndpointVerifier}负责RpcEndPoint节点查询*/
        dispatcher.registerRpcEndPoint(RpcEndpointVerifier.NAME, new RpcEndpointVerifier(this, dispatcher));
    }

    /********************************Spark RpcEndpoint Client********************************/
    public TransportClient createClient(RpcAddress address) throws IOException, InterruptedException {
        return clientFactory.createClient(address.host, address.port);

    }
    public Future<TransportClient> asyncCreateClient(RpcAddress address) {
        return clientConnectionExecutor.submit(() -> createClient(address));
    }

    /***********************************Spark网络数据序列化***************************************/
    public ByteBuffer serialize(Object content) throws IOException {
        return javaSerializerInstance.serialize(content);
    }

    public SerializationStream serializeStream(OutputStream out) throws IOException {
        return javaSerializerInstance.serializeStream(out);
    }

    public <T> T deserialize(TransportClient client, ByteBuffer buf) throws IOException {
        NettyRpcEnv.currentClient = client;
        return deserialize(() -> {
            try {
                return javaSerializerInstance.deserialize(buf);
            } catch (IOException e) {
                LOGGER.error("deserialize buf failure", e);
                throw new SparkException(e);
            }
        });
    }

    @Override
    public void awaitTermination() {
        dispatcher.awaitTermination();
    }

    @Override
    public void stop(RpcEndpointRef endPoint) {
        assert endPoint instanceof NettyRpcEndpointRef;
        dispatcher.stop(endPoint);
    }

    @Override
    public void shutdown() {
        try {
            if (!stopped.compareAndSet(false, true)) {
                return;
            }

            Iterator<Outbox> iterator = outboxes.values().iterator();
            while (iterator.hasNext()) {
                Outbox outbox = iterator.next();
                outboxes.remove(outbox.address);
                outbox.stop();
            }

            if (dispatcher != null) {
                dispatcher.stop();
            }
            if (server != null) {
                server.close();
            }
            if (clientFactory != null) {
                clientFactory.close();
            }
            if (clientConnectionExecutor != null) {
                clientConnectionExecutor.shutdownNow();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public <T> T deserialize(DeserializeAction<T> deserializeAction) {
        NettyRpcEnv.currentEnv = this;
        return deserializeAction.deserialize();
    }

    @Override
    public RpcEnvFileServer fileServer() {
        return streamManager;
    }

    @Override
    public ReadableByteChannel openChannel(String uri) {
        return null;
    }

    private List<TransportClientBootstrap> createClientBootstraps() {
        if (securityManager.isAuthenticationEnabled()) {
            return Lists.newArrayList(new AuthClientBootstrap(transportConf, securityManager.getSaslUser(), securityManager));
        }
        return Collections.emptyList();
    }

    public TransportConf getTransportConf() {
        return transportConf;
    }
}
