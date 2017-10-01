package com.sdu.spark.network.client;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.SettableFuture;
import com.sdu.spark.network.buffer.NioManagerBuffer;
import com.sdu.spark.network.protocol.*;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.sdu.spark.network.utils.NettyUtils.getRemoteAddress;

/**
 * 远端服务(RpcEndPoint)客户端
 *
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

    public ByteBuffer sendRpcSync(ByteBuffer message, long timeoutMs) {
        final SettableFuture<ByteBuffer> result = SettableFuture.create();

        sendRpc(message, new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
                ByteBuffer copy = ByteBuffer.allocate(response.remaining());
                copy.put(response);
                // flip "copy" to make it readable
                copy.flip();
                result.set(copy);
            }

            @Override
            public void onFailure(Throwable e) {
                result.setException(e);
            }
        });

        try {
            return result.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
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

    public void stream(String streamId, StreamCallback callback) {
        long startTime = System.currentTimeMillis();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Sending stream request for {} to {}", streamId, getRemoteAddress(channel));
        }

        synchronized (this) {
            responseHandler.addStreamCallback(streamId, callback);
            channel.writeAndFlush(new StreamRequest(streamId)).addListener(future -> {
                if (future.isSuccess()) {
                    long timeTaken = System.currentTimeMillis() - startTime;
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Sending request for {} to {} took {} ms", streamId,
                                getRemoteAddress(channel), timeTaken);
                    }
                } else {
                    String errorMsg = String.format("Failed to send request for %s to %s: %s", streamId,
                            getRemoteAddress(channel), future.cause());
                    LOGGER.error(errorMsg, future.cause());
                    channel.close();
                    try {
                        callback.onFailure(streamId, new IOException(errorMsg, future.cause()));
                    } catch (Exception e) {
                        LOGGER.error("Uncaught exception in RPC response callback handler!", e);
                    }
                }
            });
        }
    }

    public void fetchChunk(
            long streamId,
            int chunkIndex,
            ChunkReceivedCallback callback) {
        long startTime = System.currentTimeMillis();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Sending fetch chunk request {} to {}", chunkIndex, getRemoteAddress(channel));
        }

        StreamChunkId streamChunkId = new StreamChunkId(streamId, chunkIndex);
        responseHandler.addFetchRequest(streamChunkId, callback);

        channel.writeAndFlush(new ChunkFetchRequest(streamChunkId)).addListener(future -> {
            if (future.isSuccess()) {
                long timeTaken = System.currentTimeMillis() - startTime;
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace("Sending request {} to {} took {} ms", streamChunkId,
                            getRemoteAddress(channel), timeTaken);
                }
            } else {
                String errorMsg = String.format("Failed to send request %s to %s: %s", streamChunkId,
                        getRemoteAddress(channel), future.cause());
                LOGGER.error(errorMsg, future.cause());
                responseHandler.removeFetchRequest(streamChunkId);
                channel.close();
                try {
                    callback.onFailure(chunkIndex, new IOException(errorMsg, future.cause()));
                } catch (Exception e) {
                    LOGGER.error("Uncaught exception in RPC response callback handler!", e);
                }
            }
        });
    }

    public void removeRpcRequest(long requestId) {

    }

    @Override
    public void close() throws IOException {
        channel.close().awaitUninterruptibly(10, TimeUnit.SECONDS);
    }

    public String getClientId() {
        return this.clientId;
    }

    public void setClientId(String id) {
        Preconditions.checkState(clientId == null, "Client ID has already been set.");
        this.clientId = id;
    }

    public Channel getChannel() {
        return channel;
    }
}
