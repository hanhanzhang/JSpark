package com.sdu.spark.network.utils;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.util.concurrent.ThreadFactory;


/**
 * @author hanhan.zhang
 * */
public class NettyUtils {

    public static String getRemoteAddress(Channel channel) {
        if (channel != null && channel.remoteAddress() != null) {
            return channel.remoteAddress().toString();
        }
        return "<unknown remote>";
    }

    public static ThreadFactory createThreadFactory(String threadPoolPrefix) {
        return new DefaultThreadFactory(threadPoolPrefix, true);
    }

    public static EventLoopGroup createEventLoop(IOModel mode, int numThreads, String threadPrefix) {
        ThreadFactory threadFactory = createThreadFactory(threadPrefix);

        switch (mode) {
            case NIO:
                return new NioEventLoopGroup(numThreads, threadFactory);
            case EPOLL:
                return new EpollEventLoopGroup(numThreads, threadFactory);
            default:
                throw new IllegalArgumentException("Unknown io mode: " + mode);
        }
    }

    public static Class<? extends ServerChannel> getServerChannelClass(IOModel mode) {
        switch (mode) {
            case NIO:
                return NioServerSocketChannel.class;
            case EPOLL:
                return EpollServerSocketChannel.class;
            default:
                throw new IllegalArgumentException("Unknown io mode: " + mode);
        }
    }

}
