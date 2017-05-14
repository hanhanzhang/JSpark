package com.sdu.spark.network.utils;

import com.google.common.collect.Maps;

import java.util.Map;

/**
 * @author hanhan.zhang
 * */
public class TransportConfig {
    /**
     * IO模型[NIO, EPOLL]
     * */
    public static final String SPARK_NETWORK_IO_MODE_KEY = "io.model";
    /**
     * IO线程数
     * */
    public static final String SPARK_NETWORK_IO_SERVER_THREADS_KEY = "io.serverThreads";
    /**
     * 客户端IO线程数
     * */
    public static final String SPARK_NETWORK_IO_CLIENT_THREADS_KEY = "io.clientThreads";
    /**
     * 最大连接请求数
     * */
    public static final String SPARK_NETWORK_IO_BACKLOG_KEY = "io.backLog";
    /**
     * 最大接收Buffer
     * */
    public static final String SPARK_NETWORK_IO_RECEIVE_BUFFER_KEY = "io.receiveBuffer";
    /**
     * 最大发送Buffer
     * */
    public static final String SPARK_NETWORK_IO_SEND_BUFFER_KEY = "io.sendBuffer";
    /**
     * Server对应客户端连接数
     * */
    public static final String SPARK_NETWORK_IO_NUM_CONNECTIONS_PER_PEER_KEY = "io.numConnectionsPerPeer";

    private String module;

    private Map<String, String> conf;

    public TransportConfig(String module) {
        this.module = module;
        conf = Maps.newConcurrentMap();
    }

    public String ioModel() {
        return conf.getOrDefault(SPARK_NETWORK_IO_MODE_KEY, "nio");
    }

    public int serverThreads() {
        String threads = conf.getOrDefault(SPARK_NETWORK_IO_SERVER_THREADS_KEY, "1");
        return Integer.parseInt(threads);
    }

    public String getModuleName() {
        return module;
    }

    public int backLog() {
        String backLog = conf.getOrDefault(SPARK_NETWORK_IO_BACKLOG_KEY, "50");
        return Integer.parseInt(backLog);
    }

    public int sendBuf() {
        String sendBuf = conf.getOrDefault(SPARK_NETWORK_IO_SEND_BUFFER_KEY, "10240");
        return Integer.parseInt(sendBuf);
    }

    public int receiveBuf() {
        String receiveBuf = conf.getOrDefault(SPARK_NETWORK_IO_RECEIVE_BUFFER_KEY, "10240");
        return Integer.parseInt(receiveBuf);
    }

    public int numConnectionsPerPeer() {
        String connections = conf.getOrDefault(SPARK_NETWORK_IO_NUM_CONNECTIONS_PER_PEER_KEY, "10");
        return Integer.parseInt(connections);
    }

    public int clientThreads() {
        String clientThreads = conf.getOrDefault(SPARK_NETWORK_IO_CLIENT_THREADS_KEY, "10");
        return Integer.parseInt(clientThreads);
    }

    public boolean saslServerAlwaysEncrypt() {
        String sasl = conf.getOrDefault("spark.network.sasl.serverAlwaysEncrypt", "false");
        return Boolean.valueOf(sasl);
    }
}
