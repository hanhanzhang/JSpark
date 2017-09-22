package com.sdu.spark.network.server;

import com.sdu.spark.network.client.RpcResponseCallback;
import com.sdu.spark.network.client.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public abstract class RpcHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcHandler.class);

    private static final OneWayMessageCallback ONE_WAY_MESSAGE_CALLBACK = new OneWayMessageCallback();

    /**
     * Rpc请求处理
     * */
    public abstract void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback);

    public void receive(TransportClient client, ByteBuffer message) {
        receive(client, message, ONE_WAY_MESSAGE_CALLBACK);
    }

    public abstract StreamManager getStreamManager();

    public abstract void channelActive(TransportClient client);

    public abstract void channelInactive(TransportClient client);

    public abstract void exceptionCaught(Throwable cause, TransportClient client);


    private static class OneWayMessageCallback implements RpcResponseCallback {
        @Override
        public void onSuccess(ByteBuffer response) {
            LOGGER.warn("Response provided for one-way RPC.");
        }

        @Override
        public void onFailure(Throwable e) {
            LOGGER.warn("Error response provided for one-way RPC.", e);
        }
    }

}
