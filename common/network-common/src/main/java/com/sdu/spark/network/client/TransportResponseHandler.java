package com.sdu.spark.network.client;

import com.google.common.collect.Maps;
import com.sdu.spark.network.protocol.ResponseMessage;
import com.sdu.spark.network.protocol.RpcFailure;
import com.sdu.spark.network.protocol.RpcResponse;
import com.sdu.spark.network.server.MessageHandler;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.sdu.spark.network.utils.NettyUtils.getRemoteAddress;

/**
 *
 * @author hanhan.zhang
 * */
public class TransportResponseHandler extends MessageHandler<ResponseMessage> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransportResponseHandler.class);

    /**
     * 关联的网络通道
     * */
    private final Channel channel;
    /**
     * Rpc请求响应回调函数[key = requestId, value = callback]
     * */
    private Map<Long, RpcResponseCallback> outstandingRpcCalls;

    public TransportResponseHandler(Channel channel) {
        this.channel = channel;
        this.outstandingRpcCalls = Maps.newConcurrentMap();
    }

    public void addRpcRequest(long requestId, RpcResponseCallback callback) {
        outstandingRpcCalls.put(requestId, callback);
    }

    @Override
    public void handle(ResponseMessage message) throws Exception {
        if (message instanceof RpcResponse) {
            RpcResponse resp = (RpcResponse) message;
            RpcResponseCallback listener = outstandingRpcCalls.get(resp.requestId);
            if (listener != null) {
                outstandingRpcCalls.remove(resp.requestId);
                try {
                    listener.onSuccess(message.body().nioByteBuffer());
                } finally {
                    message.body().release();
                }
            } else {
                LOGGER.warn("Ignoring response for RPC {} from {} ({} bytes) since it is not outstanding",
                        resp.requestId, getRemoteAddress(channel), resp.body().size());
            }
        } else if (message instanceof RpcFailure) {
            RpcFailure resp = (RpcFailure) message;
            RpcResponseCallback listener = outstandingRpcCalls.get(resp.requestId);
            if (listener == null) {
                LOGGER.warn("Ignoring response for RPC {} from {} ({}) since it is not outstanding",
                        resp.requestId, getRemoteAddress(channel), resp.errorString);
            } else {
                outstandingRpcCalls.remove(resp.requestId);
                listener.onFailure(new RuntimeException(resp.errorString));
            }
        }
    }

    @Override
    public void channelActive() {

    }

    @Override
    public void exceptionCaught(Throwable cause) {

    }

    @Override
    public void channelInActive() {

    }

    public void removeRpcRequest(long requestId) {
        outstandingRpcCalls.remove(requestId);
    }
}
