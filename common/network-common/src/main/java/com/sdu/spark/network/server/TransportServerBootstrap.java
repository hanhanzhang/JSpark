package com.sdu.spark.network.server;

import io.netty.channel.Channel;

/**
 * @author hanhan.zhang
 * */
public interface TransportServerBootstrap {

    RpcHandler doBootstrap(Channel channel, RpcHandler handler);

}
