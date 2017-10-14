package com.sdu.spark.rpc.netty;

import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.network.server.StreamManager;
import com.sdu.spark.rpc.RpcEnvFileServer;
import io.netty.channel.Channel;

import java.io.File;

/**
 * @author hanhan.zhang
 * */
public class NettyStreamManager implements StreamManager, RpcEnvFileServer {

    private NettyRpcEnv nettyEnv;

    public NettyStreamManager(NettyRpcEnv nettyEnv) {
        this.nettyEnv = nettyEnv;
    }

    @Override
    public ManagedBuffer getChunk(long streamId, int chunkIndex) {
        return null;
    }

    @Override
    public ManagedBuffer openStream(String streamId) {
        return null;
    }

    @Override
    public void registerChannel(Channel channel, long streamId) {

    }

    @Override
    public void connectionTerminated(Channel channel) {

    }

    @Override
    public long chunksBeingTransferred() {
        return 0;
    }

    @Override
    public void checkAuthorization(TransportClient client, long streamId) {

    }

    @Override
    public void chunkBeingSent(long streamId) {

    }

    @Override
    public void streamBeingSent(String streamId) {

    }

    @Override
    public void chunkSent(long streamId) {

    }

    @Override
    public void streamSent(String streamId) {

    }

    @Override
    public String addFile(File file) {
        return null;
    }

    @Override
    public String addJar(File file) {
        return null;
    }

    @Override
    public String addDirectory(String baseUri, File path) {
        return null;
    }
}
