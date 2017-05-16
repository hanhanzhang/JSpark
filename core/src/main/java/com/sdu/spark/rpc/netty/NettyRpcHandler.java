package com.sdu.spark.rpc.netty;

import com.sdu.spark.network.client.RpcResponseCallback;
import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.network.server.RpcHandler;
import com.sdu.spark.network.server.StreamManager;
import com.sdu.spark.rpc.RpcAddress;
import com.sdu.spark.rpc.netty.IndexMessage.RemoteProcessConnect;
import com.sdu.spark.rpc.netty.IndexMessage.RemoteProcessConnectionError;
import com.sdu.spark.rpc.netty.IndexMessage.RemoteProcessDisconnect;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * RpcEnv Server网络请求处理
 *
 * @author hanhan.zhang
 * */
public class NettyRpcHandler extends RpcHandler{

    private StreamManager streamManager;
    private Dispatcher dispatcher;
    private NettyRpcEnv rpcEnv;

    public NettyRpcHandler(StreamManager streamManager, Dispatcher dispatcher, NettyRpcEnv rpcEnv) {
        this.streamManager = streamManager;
        this.dispatcher = dispatcher;
        this.rpcEnv = rpcEnv;
    }

    @Override
    public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
        RequestMessage request = RequestMessage.deserialize(message, rpcEnv, client);
        // 投递信息
        dispatcher.postRemoteMessage(request, callback);
    }

    @Override
    public void receive(TransportClient client, ByteBuffer message) {
        RequestMessage request = RequestMessage.deserialize(message, rpcEnv, client);
        dispatcher.postOneWayMessage(request);
    }

    @Override
    public StreamManager getStreamManager() {
        return streamManager;
    }

    @Override
    public void channelActive(TransportClient client) {
        InetSocketAddress remoteAddress = (InetSocketAddress) client.getChannel().remoteAddress();
        assert(remoteAddress != null);
        RpcAddress clientAddress = new RpcAddress(remoteAddress.getHostString(), remoteAddress.getPort());
        dispatcher.postToAll(new RemoteProcessConnect(clientAddress));
    }

    @Override
    public void channelInactive(TransportClient client) {
        InetSocketAddress remoteAddress = (InetSocketAddress) client.getChannel().remoteAddress();
        if (remoteAddress != null) {
            RpcAddress clientAddress = new RpcAddress(remoteAddress.getHostString(), remoteAddress.getPort());
            rpcEnv.removeOutbox(clientAddress);
            dispatcher.postToAll(new RemoteProcessDisconnect(clientAddress));
        }
    }

    @Override
    public void exceptionCaught(Throwable cause, TransportClient client) {
        InetSocketAddress remoteAddress = (InetSocketAddress) client.getChannel().remoteAddress();
        if (remoteAddress != null) {
            RpcAddress clientAddress = new RpcAddress(remoteAddress.getHostString(), remoteAddress.getPort());
            dispatcher.postToAll(new RemoteProcessConnectionError(cause, clientAddress));
        }
    }
}
