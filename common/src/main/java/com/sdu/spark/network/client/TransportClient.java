package com.sdu.spark.network.client;

import com.google.common.base.Preconditions;
import com.sdu.spark.network.buffer.NioManagerBuffer;
import com.sdu.spark.network.protocol.OneWayMessage;
import com.sdu.spark.network.protocol.RpcRequest;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.sdu.spark.network.utils.NettyUtils.getRemoteAddress;

/**
 * @author hanhan.zhang
 * */
public class TransportClient implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransportClient.class);

    private Channel channel;

    private TransportResponseHandler responseHandler;
    private String clientId;

    public TransportClient(Channel channel, TransportResponseHandler responseHandler) {
        this.channel = channel;
        this.responseHandler = responseHandler;
    }

    public boolean isActive() {
        return channel.isOpen() && channel.isActive();
    }

    /**
     * 发送单向消息
     * */
    public void send(ByteBuffer message) {
        channel.writeAndFlush(new OneWayMessage(new NioManagerBuffer(message)));
    }

    /**
     * 发送双向消息
     * */
    public long sendRpc(ByteBuffer message, RpcResponseCallback callback) {
        long startTime = System.currentTimeMillis();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Sending RPC to {}", getRemoteAddress(channel));
        }
        long requestId = Math.abs(UUID.randomUUID().getLeastSignificantBits());
        responseHandler.addRpcRequest(requestId, callback);
        channel.writeAndFlush(new RpcRequest(requestId, new NioManagerBuffer(message)))
                .addListener(future -> {
                    if (future.isSuccess()) {
                        long timeTaken = System.currentTimeMillis() - startTime;
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace("Sending request {} to {} took {} ms", requestId,
                                    getRemoteAddress(channel), timeTaken);
                        }
                    } else {
                        String errorMsg = String.format("Failed to send RPC %s to %s: %s", requestId,
                                getRemoteAddress(channel), future.cause());
                        LOGGER.error(errorMsg, future.cause());
                        responseHandler.removeRpcRequest(requestId);
                        channel.close();
                        try {
                            callback.onFailure(new IOException(errorMsg, future.cause()));
                        } catch (Exception e) {
                            LOGGER.error("Uncaught exception in RPC response callback handler!", e);
                        }
                    }
                });
        return requestId;
    }

    public void removeRpcRequest(long requestId) {

    }

    @Override
    public void close() throws IOException {
        channel.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
    }

    public void setClientId(String id) {
        Preconditions.checkState(clientId == null, "Client ID has already been set.");
        this.clientId = id;
    }
}
