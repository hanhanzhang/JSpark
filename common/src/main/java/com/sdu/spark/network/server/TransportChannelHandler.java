package com.sdu.spark.network.server;

import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.network.client.TransportResponseHandler;
import com.sdu.spark.network.protocol.RequestMessage;
import com.sdu.spark.network.protocol.ResponseMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.sdu.spark.network.utils.NettyUtils.getRemoteAddress;

/**
 * Transport Server消息处理
 *
 * @author hanhan.zhang
 * */
public class TransportChannelHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransportChannelHandler.class);

    private final TransportClient client;
    private final TransportRequestHandler requestHandler;
    private final TransportResponseHandler responseHandler;
    private final boolean closeIdleConnections;

    public TransportChannelHandler(TransportClient client, TransportRequestHandler requestHandler, TransportResponseHandler responseHandler, boolean closeIdleConnections) {
        this.client = client;
        this.requestHandler = requestHandler;
        this.responseHandler = responseHandler;
        this.closeIdleConnections = closeIdleConnections;
    }

    public TransportClient getClient() {
        return client;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        try {
            requestHandler.channelActive();
        } catch (RuntimeException e) {
            LOGGER.error("Exception from request handler while channel is active", e);
        }
        try {
            responseHandler.channelActive();
        } catch (RuntimeException e) {
            LOGGER.error("Exception from response handler while channel is active", e);
        }
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        try {
            requestHandler.channelInActive();
        } catch (RuntimeException e) {
            LOGGER.error("Exception from request handler while channel is inactive", e);
        }
        try {
            responseHandler.channelInActive();
        } catch (RuntimeException e) {
            LOGGER.error("Exception from response handler while channel is inactive", e);
        }
        super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof RequestMessage) {
            requestHandler.handle((RequestMessage) msg);
        } else if (msg instanceof ResponseMessage) {
            responseHandler.handle((ResponseMessage) msg);
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOGGER.warn("Exception in connection from " + getRemoteAddress(ctx.channel()), cause);
        requestHandler.exceptionCaught(cause);
        responseHandler.exceptionCaught(cause);
        ctx.close();
    }
}
