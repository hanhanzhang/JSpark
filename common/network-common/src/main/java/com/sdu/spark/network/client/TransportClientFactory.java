package com.sdu.spark.network.client;

import com.sdu.spark.network.TransportContext;
import com.sdu.spark.network.server.TransportChannelHandler;
import com.sdu.spark.network.utils.IOModel;
import com.sdu.spark.network.utils.JavaUtils;
import com.sdu.spark.network.utils.NettyUtils;
import com.sdu.spark.network.utils.TransportConf;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author hanhan.zhang
 * */
public class TransportClientFactory implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransportClientFactory.class);

    private TransportContext context;
    private TransportConf conf;
    private List<TransportClientBootstrap> clientBootstraps;

    /**
     * 服务端连接池[key = 服务端地址, value = 连接池]
     * */
    private final ConcurrentHashMap<SocketAddress, ClientPool> connectionPool;
    /**
     * 负载均衡
     * */
    private Random random = new Random();

    /**
     * 多个客户端共用
     * */
    private EventLoopGroup workerGroup;
    private final Class<? extends Channel> socketChannelClass;



    /**
     * 客户端连接池
     * */
    private static class ClientPool {

        TransportClient []clients;
        Object []locks;

        public ClientPool(int size) {
            clients = new TransportClient[size];
            locks = new Object[size];
            for (int i = 0; i < size; ++i) {
                locks[i] = new Object();
            }
        }
    }

    public TransportClientFactory(TransportContext context, List<TransportClientBootstrap> clientBootstraps) {
        this.context = context;
        this.conf = context.getConf();
        this.clientBootstraps = clientBootstraps;
        this.connectionPool = new ConcurrentHashMap<>();
        this.socketChannelClass = NettyUtils.getClientChannelClass(IOModel.convert(conf.ioModel()));
        this.workerGroup = NettyUtils.createEventLoop(IOModel.convert(conf.ioModel()), conf.clientThreads(),
                conf.getModuleName() + "-client");
    }

    /**
     * 创建连接
     *
     * 1: 先在连接池中取
     *
     * 2: 连接池中不存在, 则创建连接
     * */
    public TransportClient createClient(String remoteHost, int remotePort) throws IOException, InterruptedException {
        final InetSocketAddress address = InetSocketAddress.createUnresolved(remoteHost, remotePort);
        ClientPool clientPool = connectionPool.get(address);
        if (clientPool == null) {
            connectionPool.putIfAbsent(address, new ClientPool(conf.numConnectionsPerPeer()));
            clientPool = connectionPool.get(address);
        }
        int clientIndex = random.nextInt(conf.numConnectionsPerPeer());
        TransportClient client = clientPool.clients[clientIndex];
        if (client != null && client.isActive()) {
            return client;
        }

        final long preResolveHost = System.nanoTime();
        final InetSocketAddress resolvedAddress = new InetSocketAddress(remoteHost, remotePort);
        final long hostResolveTimeMs = (System.nanoTime() - preResolveHost) / 1000000;
        if (hostResolveTimeMs > 2000) {
            LOGGER.warn("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
        } else {
            LOGGER.trace("DNS resolution for {} took {} ms", resolvedAddress, hostResolveTimeMs);
        }

        synchronized (clientPool.locks[clientIndex]) {
            client = clientPool.clients[clientIndex];
            if (client != null && client.isActive()) {
                return client;
            }
            clientPool.clients[clientIndex] = createClient(resolvedAddress);
            return clientPool.clients[clientIndex];
        }
    }

    private TransportClient createClient(InetSocketAddress address) {
        LOGGER.debug("Creating new connection to {}", address);

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup)
                .channel(socketChannelClass)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 120)
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

        final AtomicReference<TransportClient> clientRef = new AtomicReference<>();
        final AtomicReference<Channel> channelRef = new AtomicReference<>();

        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) {
                TransportChannelHandler clientHandler = context.initializePipeline(ch);
                clientRef.set(clientHandler.getClient());
                channelRef.set(ch);
            }
        });

        // Connect to the remote server
        long preConnect = System.nanoTime();
        ChannelFuture cf = bootstrap.connect(address);

        try {
            if (!cf.await(1000L)) {
                throw new RuntimeException(String.format("Connecting to %s timed out (%s ms)", address, 1000));
            } else if (cf.cause() != null) {
                throw new RuntimeException(String.format("Failed to connect to %s", address), cf.cause());
            }
        } catch (InterruptedException e) {
            // Ignore
            e.printStackTrace();
        }


        TransportClient client = clientRef.get();
        Channel channel = channelRef.get();
        assert client != null : "Channel future completed successfully with null client";

        // Execute any client bootstraps synchronously before marking the Client as successful.
        long preBootstrap = System.nanoTime();
        LOGGER.debug("Connection to {} successful, running bootstraps...", address);
        try {
            for (TransportClientBootstrap clientBootstrap : clientBootstraps) {
                clientBootstrap.doBootstrap(client, channel);
            }
        } catch (Exception e) { // catch non-RuntimeExceptions too as bootstrap may be written in Scala
            long bootstrapTimeMs = (System.nanoTime() - preBootstrap) / 1000000;
            LOGGER.error("Exception while bootstrapping client after " + bootstrapTimeMs + " ms", e);
            try {
                client.close();
            } catch (IOException e1) {
                //
            }
        }
        long postBootstrap = System.nanoTime();

        LOGGER.info("Successfully created connection to {} after {} ms ({} ms spent in bootstraps)",
                address, (postBootstrap - preConnect) / 1000000, (postBootstrap - preBootstrap) / 1000000);

        return client;
    }

    @Override
    public void close() throws IOException {
        for (ClientPool clientPool : connectionPool.values()) {
            for (int i = 0; i < clientPool.clients.length; i++) {
                TransportClient client = clientPool.clients[i];
                if (client != null) {
                    clientPool.clients[i] = null;
                    JavaUtils.closeQuietly(client);
                }
            }
        }
        connectionPool.clear();

        if (workerGroup != null) {
            workerGroup.shutdownGracefully();
            workerGroup = null;
        }
    }
}
