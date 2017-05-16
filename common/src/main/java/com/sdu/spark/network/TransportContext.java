package com.sdu.spark.network;

import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.network.client.TransportClientBootstrap;
import com.sdu.spark.network.client.TransportClientFactory;
import com.sdu.spark.network.client.TransportResponseHandler;
import com.sdu.spark.network.protocol.MessageDecoder;
import com.sdu.spark.network.protocol.MessageEncoder;
import com.sdu.spark.network.server.*;
import com.sdu.spark.network.utils.TransportConfig;
import io.netty.channel.Channel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * {@link TransportContext}负责创建{@link TransportServer}和{@link TransportClientFactory}
 *
 * @author hanhan.zhang
 * */
public class TransportContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransportContext.class);

    private final TransportConfig conf;
    private final RpcHandler rpcHandler;
    private final boolean closeIdleConnections;

    /**
     * RpcMessage编码/解码
     * */
    private static final MessageEncoder ENCODER = MessageEncoder.INSTANCE;
    private static final MessageDecoder DECODER = MessageDecoder.INSTANCE;

    public TransportContext(TransportConfig conf, RpcHandler rpcHandler) {
        this(conf, rpcHandler, false);
    }

    public TransportContext(TransportConfig conf, RpcHandler rpcHandler, boolean closeIdleConnections) {
        this.conf = conf;
        this.rpcHandler = rpcHandler;
        this.closeIdleConnections = closeIdleConnections;
    }

    public TransportClientFactory createClientFactory() {
        return createClientFactory(Collections.emptyList());
    }

    public TransportClientFactory createClientFactory(List<TransportClientBootstrap> clientBootstraps) {
        return new TransportClientFactory(this, clientBootstraps);
    }

    public TransportServer createServer(int port, List<TransportServerBootstrap> serverBootstraps) {
        return createServer(null, port, serverBootstraps);
    }

    public TransportServer createServer(String host, int port, List<TransportServerBootstrap> serverBootstraps) {
        return new TransportServer(this, host, port, rpcHandler, serverBootstraps);
    }

    public TransportConfig getConf() {
        return conf;
    }

    public TransportChannelHandler initializePipeline(SocketChannel channel) {
        return initializePipeline(channel, rpcHandler);
    }

    /**
     * 初始化ChannelHandler
     * */
    public TransportChannelHandler  initializePipeline(
            SocketChannel channel,
            RpcHandler channelRpcHandler) {
        try {
            TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);
            channel.pipeline()
                    .addLast("encoder", ENCODER)
//                    .addLast(TransportFrameDecoder.HANDLER_NAME, NettyUtils.createFrameDecoder())
                    .addLast("decoder", DECODER)
                    .addLast("idleStateHandler", new IdleStateHandler(0, 0, 120))
                    // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
                    // would require more logic to guarantee if this were not part of the same event loop.
                    .addLast("handler", channelHandler);
            return channelHandler;
        } catch (RuntimeException e) {
            LOGGER.error("Error while initializing Netty pipeline", e);
            throw e;
        }
    }

    private TransportChannelHandler createChannelHandler(Channel channel, RpcHandler rpcHandler) {
        TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
        TransportClient client = new TransportClient(channel, responseHandler);
        TransportRequestHandler requestHandler = new TransportRequestHandler(channel, client, rpcHandler);
        return new TransportChannelHandler(client, requestHandler, responseHandler, closeIdleConnections);
    }
}
