package com.sdu.spark.network.server;

import com.google.common.base.Throwables;
import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.network.buffer.NioManagerBuffer;
import com.sdu.spark.network.client.RpcResponseCallback;
import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.network.protocol.*;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import static com.sdu.spark.network.utils.NettyUtils.getRemoteAddress;

/**
 * {@link RpcRequest}消息处理, 三类消息:
 *
 *  1: {@link RpcRequest}
 *
 *  2: {@link ChunkFetchRequest}
 *  
 *  3: {@link StreamRequest}
 *
 * @author hanhan.zhang
 * */
public class TransportRequestHandler extends MessageHandler<RequestMessage> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransportRequestHandler.class);

    private Channel channel;
    private TransportClient reverseClient;

    // 消息处理接口
    private RpcHandler rpcHandler;

    // 数据块请求
    private StreamManager streamManager;
    // 允许最大传输chunk数
    private long maxChunksBeingTransferred;

    public TransportRequestHandler(Channel channel, TransportClient reverseClient,
                                   RpcHandler rpcHandler, long maxChunksBeingTransferred) {
        this.channel = channel;
        this.reverseClient = reverseClient;
        this.rpcHandler = rpcHandler;
        this.streamManager = rpcHandler.getStreamManager();
        this.maxChunksBeingTransferred = maxChunksBeingTransferred;
    }

    @Override
    public void handle(RequestMessage message) throws Exception {
        if (message instanceof ChunkFetchRequest) {
            processFetchRequest((ChunkFetchRequest) message);
        } else if (message instanceof RpcRequest) {
            processRpcRequest((RpcRequest) message);
        } else if (message instanceof OneWayMessage) {
            processOneWayMessage((OneWayMessage) message);
        } else if (message instanceof StreamRequest) {
            processStreamRequest((StreamRequest) message);
        }
    }

    @Override
    public void channelActive() {
        rpcHandler.channelActive(reverseClient);
    }

    @Override
    public void exceptionCaught(Throwable cause) {
        rpcHandler.exceptionCaught(cause, reverseClient);
    }

    @Override
    public void channelInActive() {
        if (streamManager != null) {
            try {
                streamManager.connectionTerminated(channel);
            } catch (RuntimeException e) {
                LOGGER.error("StreamManager connectionTerminated() callback failed.", e);
            }
        }
        rpcHandler.channelInactive(reverseClient);
    }

    private void processFetchRequest(ChunkFetchRequest req) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Received req from {} to fetch block {}", getRemoteAddress(channel), req.streamChunkId);
        }
        long chunksBeingTransferred = streamManager.chunksBeingTransferred();
        if (chunksBeingTransferred >= maxChunksBeingTransferred) {
            LOGGER.warn("The number of chunks being transferred {} is above {}, close the connection.",
                    chunksBeingTransferred, maxChunksBeingTransferred);
            channel.close();
            return;
        }
        ManagedBuffer buf;
        try {
            streamManager.checkAuthorization(reverseClient, req.streamChunkId.streamId);
            streamManager.registerChannel(channel, req.streamChunkId.streamId);
            buf = streamManager.getChunk(req.streamChunkId.streamId, req.streamChunkId.chunkIndex);
        } catch (Exception e) {
            LOGGER.error(String.format("Error opening block %s for request from %s",
                    req.streamChunkId, getRemoteAddress(channel)), e);
            respond(new ChunkFetchFailure(req.streamChunkId, Throwables.getStackTraceAsString(e)));
            return;
        }

        streamManager.chunkBeingSent(req.streamChunkId.streamId);
        respond(new ChunkFetchSuccess(req.streamChunkId, buf)).addListener(future -> {
            streamManager.chunkSent(req.streamChunkId.streamId);
        });

    }

    private void processRpcRequest(RpcRequest req) {
        try {
            rpcHandler.receive(reverseClient, req.body().nioByteBuffer(), new RpcResponseCallback() {
                @Override
                public void onSuccess(ByteBuffer response) {
                    respond(new RpcResponse(req.requestId, new NioManagerBuffer(response)));
                }

                @Override
                public void onFailure(Throwable e) {
                    respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
                }
            });
        } catch (IOException e) {
            LOGGER.error("Error while invoking RpcHandler#receive() on RPC id {}", req.requestId, e);
            respond(new RpcFailure(req.requestId, Throwables.getStackTraceAsString(e)));
        } finally {
            // 释放内存
            req.body().release();
        }
    }

    private void processOneWayMessage(OneWayMessage req) {
        try {
            rpcHandler.receive(reverseClient, req.body().nioByteBuffer());
        } catch (IOException e) {
            LOGGER.error("Error while invoking RpcHandler#receive() for one-way message.", e);
        } finally {
            req.body().release();
        }
    }

    private void processStreamRequest(StreamRequest req) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Received req from {} to fetch stream {}", getRemoteAddress(channel),
                    req.streamId);
        }
        long chunksBeingTransferred =  streamManager.chunksBeingTransferred();
        if (chunksBeingTransferred >= maxChunksBeingTransferred) {
            LOGGER.warn("The number of chunks being transferred {} is above {}, close the connection.",
                    chunksBeingTransferred, maxChunksBeingTransferred);
            channel.close();
            return;
        }

        ManagedBuffer buf;

        try {
            buf = streamManager.openStream(req.streamId);
        } catch (Exception e) {
            LOGGER.error(String.format(
                    "Error opening stream %s for request from %s", req.streamId, getRemoteAddress(channel)), e);
            respond(new StreamFailure(req.streamId, Throwables.getStackTraceAsString(e)));
            return;
        }

        if (buf != null) {
            streamManager.streamBeingSent(req.streamId);
            respond(new StreamResponse(req.streamId, buf.size(), buf)).addListener(future -> {
                streamManager.streamSent(req.streamId);
            });
        } else {
            respond(new StreamFailure(req.streamId, String.format("Stream '%s' was not found.", req.streamId)));
        }
    }

    /**
     * 响应Rpc请求
     * */
    private ChannelFuture respond(Encodable result) {
        SocketAddress remoteAddress = channel.remoteAddress();
        return channel.writeAndFlush(result).addListener(future -> {
            if (future.isSuccess()) {
                LOGGER.trace("Sent result {} to client {}", result, remoteAddress);
            } else {
                LOGGER.error(String.format("Error sending result %s to %s; closing connection",
                        result, remoteAddress), future.cause());
                channel.close();
            }
        });
    }
}
