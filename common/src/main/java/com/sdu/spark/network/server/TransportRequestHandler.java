package com.sdu.spark.network.server;

import com.google.common.base.Throwables;
import com.sdu.spark.network.buffer.NioManagerBuffer;
import com.sdu.spark.network.client.RpcResponseCallback;
import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.network.protocol.*;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

/**
 * 请求处理
 *
 * @author hanhan.zhang
 * */
public class TransportRequestHandler extends MessageHandler<RequestMessage> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransportRequestHandler.class);

    /**
     * 关联的网络通道
     * */
    private Channel channel;
    /**
     * 请求客户端
     * */
    private TransportClient reverseClient;
    /**
     * 消息处理
     * */
    private RpcHandler rpcHandler;
    /**
     * 大数据网络传输处理
     * */
    private StreamManager streamManager;

    public TransportRequestHandler(Channel channel, TransportClient reverseClient, RpcHandler rpcHandler) {
        this.channel = channel;
        this.reverseClient = reverseClient;
        this.rpcHandler = rpcHandler;
        this.streamManager = rpcHandler.getStreamManager();
    }

    @Override
    public void handle(RequestMessage message) throws Exception {
        if (message instanceof RpcRequest) {
            processRpcRequest((RpcRequest) message);
        } else if (message instanceof OneWayMessage) {
            processOneWayMessage((OneWayMessage) message);
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

    /**
     * 响应Rpc请求
     * */
    private void respond(Encodable result) {
        SocketAddress remoteAddress = channel.remoteAddress();
        channel.writeAndFlush(result).addListener(future -> {
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
