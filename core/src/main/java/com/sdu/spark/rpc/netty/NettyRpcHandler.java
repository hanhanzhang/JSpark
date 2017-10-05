package com.sdu.spark.rpc.netty;

import com.google.common.collect.Maps;
import com.sdu.spark.network.client.RpcResponseCallback;
import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.network.server.RpcHandler;
import com.sdu.spark.network.server.StreamManager;
import com.sdu.spark.rpc.RpcAddress;
import com.sdu.spark.rpc.netty.IndexMessage.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;

/**
 * RpcEnv Server网络请求处理
 *
 * @author hanhan.zhang
 * */
public class NettyRpcHandler extends RpcHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyRpcHandler.class);

    private StreamManager streamManager;
    private Dispatcher dispatcher;
    private NettyRpcEnv nettyEnv;

    // 维护Remote Server地址信息, key = channel remote address, value = 真实的远端地址
    private ConcurrentMap<RpcAddress, RpcAddress> remoteAddresses = Maps.newConcurrentMap();

    public NettyRpcHandler(StreamManager streamManager, Dispatcher dispatcher, NettyRpcEnv nettyEnv) {
        this.streamManager = streamManager;
        this.dispatcher = dispatcher;
        this.nettyEnv = nettyEnv;
    }

    @Override
    public void receive(TransportClient client, ByteBuffer message, RpcResponseCallback callback) {
        RequestMessage messageToDispatch = internalReceive(client, message);
        // 投递信息
        dispatcher.postRemoteMessage(messageToDispatch, callback);
    }

    @Override
    public void receive(TransportClient client, ByteBuffer message) {
        RequestMessage messageToDispatch = internalReceive(client, message);
        dispatcher.postOneWayMessage(messageToDispatch);
    }

    @Override
    public StreamManager getStreamManager() {
        return streamManager;
    }

    @Override
    public void channelActive(TransportClient client) {
        InetSocketAddress remoteAddress = (InetSocketAddress) client.getChannel().remoteAddress();
        assert(remoteAddress != null);
        RpcAddress clientAddr = new RpcAddress(remoteAddress.getHostString(), remoteAddress.getPort());
        dispatcher.postToAll(new RemoteProcessConnected(clientAddr));
    }

    @Override
    public void channelInactive(TransportClient client) {
        InetSocketAddress addr = (InetSocketAddress) client.getChannel().remoteAddress();
        if (addr != null) {
            RpcAddress clientAddr = new RpcAddress(addr.getHostString(), addr.getPort());
            nettyEnv.removeOutbox(clientAddr);
//            dispatcher.postToAll(new RemoteProcessDisconnected(clientAddr));
            RpcAddress remoteEnvAddress = remoteAddresses.remove(clientAddr);
            // If the remove RpcEnv listens to some address, we should also  fire a
            // RemoteProcessDisconnected for the remote RpcEnv listening address
            if (remoteEnvAddress != null) {
                dispatcher.postToAll(new RemoteProcessDisconnected(remoteEnvAddress));
            }
        } else {
            // If the channel is closed before connecting, its remoteAddress will be null. In this case,
            // we can ignore it since we don't fire "Associated".
            // See java.net.Socket.getRemoteSocketAddress
        }
    }

    @Override
    public void exceptionCaught(Throwable cause, TransportClient client) {
        InetSocketAddress remote = (InetSocketAddress) client.getChannel().remoteAddress();
        if (remote != null) {
            RpcAddress clientAddr = new RpcAddress(remote.getHostString(), remote.getPort());
            dispatcher.postToAll(new RemoteProcessConnectionError(cause, clientAddr));
            // If the remove RpcEnv listens to some address, we should also fire a
            // RemoteProcessConnectionError for the remote RpcEnv listening address
            RpcAddress remoteEnvAddress = remoteAddresses.get(clientAddr);
            if (remoteEnvAddress != null) {
                dispatcher.postToAll(new RemoteProcessConnectionError(cause, remoteEnvAddress));
            }
        } else {
            // If the channel is closed before connecting, its remoteAddress will be null.
            // See java.net.Socket.getRemoteSocketAddress
            // Because we cannot get a RpcAddress, just log it
            LOGGER.error("Exception before connecting to the client", cause);
        }
    }

    private RequestMessage internalReceive(TransportClient client, ByteBuffer message) {
        InetSocketAddress addr = (InetSocketAddress) client.getChannel().remoteAddress();
        assert(addr != null);
        RpcAddress clientAddr = new RpcAddress(addr.getHostString(), addr.getPort());
        RequestMessage requestMessage = RequestMessage.deserialize(message, nettyEnv, client);
        if (requestMessage.senderAddress == null) {
            // Create a new message with the socket address of the client as the sender.
            return new RequestMessage(clientAddr, requestMessage.receiver, requestMessage.content);
        } else {
            // The remote RpcEnv listens to some port, we should also fire a RemoteProcessConnected for
            // the listening address
            RpcAddress remoteEnvAddress = requestMessage.senderAddress;
            if (remoteAddresses.putIfAbsent(clientAddr, remoteEnvAddress) == null) {
                dispatcher.postToAll(new RemoteProcessConnected(remoteEnvAddress));
            }
            return requestMessage;
        }
    }
}
