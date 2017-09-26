package com.sdu.spark.network.server;

import com.sdu.spark.network.client.RpcResponseCallback;
import com.sdu.spark.network.client.TransportClient;

import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public class NoOpRpcHandler extends RpcHandler {

    private final StreamManager streamManager;

    public NoOpRpcHandler() {
        this.streamManager = new OneForOneStreamManager();
    }

    @Override
    public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
        throw new UnsupportedOperationException("Cannot handle messages");
    }

    @Override
    public StreamManager getStreamManager() {
        return streamManager;
    }

    @Override
    public void channelActive(TransportClient client) {

    }

    @Override
    public void channelInactive(TransportClient client) {

    }

    @Override
    public void exceptionCaught(Throwable cause, TransportClient client) {

    }
}
