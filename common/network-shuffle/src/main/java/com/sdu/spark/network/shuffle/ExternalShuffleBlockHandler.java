package com.sdu.spark.network.shuffle;

import com.sdu.spark.network.client.RpcResponseCallback;
import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.network.server.RpcHandler;
import com.sdu.spark.network.server.StreamManager;

import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public class ExternalShuffleBlockHandler extends RpcHandler {
    public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {

    }

    public StreamManager getStreamManager() {
        return null;
    }

    public void channelActive(TransportClient client) {

    }

    public void channelInactive(TransportClient client) {

    }

    public void exceptionCaught(Throwable cause, TransportClient client) {

    }
}
