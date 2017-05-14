package com.sdu.spark.network.server;

import io.netty.channel.Channel;

/**
 * @author hanhan.zhang
 * */
public abstract class StreamManager {

    public void connectionTerminated(Channel channel){};
}
