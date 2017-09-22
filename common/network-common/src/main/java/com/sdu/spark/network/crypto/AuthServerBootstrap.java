package com.sdu.spark.network.crypto;

import com.sdu.spark.network.sasl.SecretKeyHolder;
import com.sdu.spark.network.server.RpcHandler;
import com.sdu.spark.network.server.TransportServerBootstrap;
import com.sdu.spark.network.utils.TransportConf;
import io.netty.channel.Channel;

/**
 * @author hanhan.zhang
 * */
public class AuthServerBootstrap implements TransportServerBootstrap {

    private final TransportConf conf;
    private final SecretKeyHolder secretKeyHolder;

    public AuthServerBootstrap(TransportConf conf, SecretKeyHolder secretKeyHolder) {
        this.conf = conf;
        this.secretKeyHolder = secretKeyHolder;
    }

    @Override
    public RpcHandler doBootstrap(Channel channel, RpcHandler handler) {
        return null;
    }
}
