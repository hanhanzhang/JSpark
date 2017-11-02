package com.sdu.spark.network.server;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.sdu.spark.network.TransportContext;
import com.sdu.spark.network.utils.IOModel;
import com.sdu.spark.network.utils.JavaUtils;
import com.sdu.spark.network.utils.NettyUtils;
import com.sdu.spark.network.utils.TransportConf;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * JSpark RpcEnv Server负责处理远端RpcEndPoint数据请求
 *
 * @author hanhan.zhang
 * */
public class TransportServer implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransportServer.class);

    private TransportContext context;
    private TransportConf conf;
    private RpcHandler appRpcHandler;
    private List<TransportServerBootstrap> bootstraps;

    private ServerBootstrap bootstrap;
    private ChannelFuture channelFuture;
    private int port = -1;

    public TransportServer(TransportContext context, String hostToBind, int portToBind, RpcHandler appRpcHandler, List<TransportServerBootstrap> serverBootstraps) {
        this.context = context;
        this.conf = context.getConf();
        this.appRpcHandler = appRpcHandler;
        this.bootstraps = Lists.newArrayList(Preconditions.checkNotNull(serverBootstraps));

        try {
            init(hostToBind, portToBind);
        } catch (RuntimeException e) {
            JavaUtils.closeQuietly(this);
            throw e;
        }
    }

    private void init(String hostToBind, int portToBind) {
        IOModel ioModel = IOModel.convert(conf.ioModel());
        EventLoopGroup bossGroup =
                NettyUtils.createEventLoop(ioModel, conf.serverThreads(), conf.getModuleName() + "-server");
        EventLoopGroup workerGroup = bossGroup;

        bootstrap = new ServerBootstrap().group(bossGroup, workerGroup)
                .channel(NettyUtils.getServerChannelClass(ioModel))
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

        if (conf.backLog() > 0) {
            bootstrap.option(ChannelOption.SO_BACKLOG, conf.backLog());
        }

        if (conf.receiveBuf() > 0) {
            bootstrap.childOption(ChannelOption.SO_RCVBUF, conf.receiveBuf());
        }

        if (conf.sendBuf() > 0) {
            bootstrap.childOption(ChannelOption.SO_SNDBUF, conf.sendBuf());
        }

        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                RpcHandler rpcHandler = appRpcHandler;
                if (bootstraps != null) {
                   for (TransportServerBootstrap serverBootstrap : bootstraps) {
                       rpcHandler = serverBootstrap.doBootstrap(ch, rpcHandler);
                   }
                }
                context.initializePipeline(ch, rpcHandler);
            }
        });

        InetSocketAddress address = hostToBind == null ?
                new InetSocketAddress(portToBind): new InetSocketAddress(hostToBind, portToBind);
        channelFuture = bootstrap.bind(address);
        channelFuture.syncUninterruptibly();

        port = ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
        LOGGER.debug("Shuffle server started on port: {}", port);
    }

    public int getPort() {
        assert port != -1 : "Server not initialized";
        return port;
    }

    @Override
    public void close() throws IOException {
        if (channelFuture != null) {
            // close is a local operation and should finish within milliseconds; timeout just to be safe
            channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS);
            channelFuture = null;
        }
        if (bootstrap != null && bootstrap.group() != null) {
            bootstrap.group().shutdownGracefully();
        }
        if (bootstrap != null && bootstrap.childGroup() != null) {
            bootstrap.childGroup().shutdownGracefully();
        }
        bootstrap = null;
    }
}
