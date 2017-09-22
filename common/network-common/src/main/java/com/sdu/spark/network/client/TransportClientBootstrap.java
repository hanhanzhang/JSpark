package com.sdu.spark.network.client;

import io.netty.channel.Channel;

/**
 * @author hanhan.zhang
 * */
public interface TransportClientBootstrap {

    void doBootstrap(TransportClient client, Channel channel) throws RuntimeException;

}
