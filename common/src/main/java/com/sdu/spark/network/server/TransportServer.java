package com.sdu.spark.network.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * JSpark RpcEnv Server负责处理远端RpcEndPoint数据请求
 *
 * @author hanhan.zhang
 * */
public class TransportServer implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransportServer.class);

    private RpcHandler appRpcHandler;
    private List<TransportServerBootstrap> bootstraps;

    private ServerBootstrap bootstrap;
    private ChannelFuture channelFuture;
    private int port = -1;



    @Override
    public void close() throws IOException {

    }
}
