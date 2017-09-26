package com.sdu.spark.network.crypto;

import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.network.client.TransportClientBootstrap;
import com.sdu.spark.network.sasl.SecretKeyHolder;
import com.sdu.spark.network.utils.TransportConf;
import io.netty.channel.Channel;

/**
 * @author hanhan.zhang
 * */
public class AuthClientBootstrap implements TransportClientBootstrap {

    public AuthClientBootstrap(
            TransportConf conf,
            String appId,
            SecretKeyHolder secretKeyHolder) {

    }

    @Override
    public void doBootstrap(TransportClient client, Channel channel) throws RuntimeException {

    }
}
