package com.sdu.spark.network.server;

import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.network.client.TransportResponseHandler;
import com.sdu.spark.network.protocol.RequestMessage;
import com.sdu.spark.network.protocol.ResponseMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
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
    private final long requestTimeoutNs;
    private final boolean closeIdleConnections;

    public TransportChannelHandler(TransportClient client,
                                   TransportRequestHandler requestHandler,
                                   TransportResponseHandler responseHandler,
                                   long requestTimeoutMs,
                                   boolean closeIdleConnections) {
        this.client = client;
        this.requestHandler = requestHandler;
        this.responseHandler = responseHandler;
        this.requestTimeoutNs = requestTimeoutMs * 1000 * 1000L;
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
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent e = (IdleStateEvent) evt;
            // See class comment for timeout semantics. In addition to ensuring we only timeout while
            // there are outstanding requests, we also do a secondary consistency check to ensure
            // there's no race between the idle timeout and incrementing the numOutstandingRequests
            // (see SPARK-7003).
            //
            // To avoid a race between TransportClientFactory.createClient() and this code which could
            // result in an inactive client being returned, this needs to run in a synchronized block.
            synchronized (this) {
                boolean isActuallyOverdue =
                        System.nanoTime() - responseHandler.getTimeOfLastRequestNs() > requestTimeoutNs;
                if (e.state() == IdleState.ALL_IDLE && isActuallyOverdue) {
                    if (responseHandler.numOutstandingRequests() > 0) {
                        String address = getRemoteAddress(ctx.channel());
                        LOGGER.error("Connection to {} has been quiet for {} ms while there are outstanding " +
                                "requests. Assuming connection is dead; please adjust spark.network.timeout if " +
                                "this is wrong.", address, requestTimeoutNs / 1000 / 1000);
                        client.timeOut();
                        ctx.close();
                    } else if (closeIdleConnections) {
                        // While CloseIdleConnections is enable, we also close idle connection
                        client.timeOut();
                        ctx.close();
                    }
                }
            }
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
