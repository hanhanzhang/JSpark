package com.sdu.spark.network.utils;

import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.Map;

import static com.sdu.spark.network.utils.JavaUtils.timeStringAsSec;

/**
 * @author hanhan.zhang
 * */
public class TransportConf {
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
    /**
     * 数据请求重试次数
     * */
    public static final String SPARK_NETWORK_IO_MAXRETRIES_KEY = "io.maxRetries";
    /**
     * 数据请求重试等待时间
     * */
    public static final String SPARK_NETWORK_IO_RETRYWAIT_KEY = "io.retryWait";
    public static final String SPARK_NETWORK_IO_LAZYFD_KEY = "io.lazyFD";

    private String module;

    private ConfigProvider conf;

    public TransportConf(String module, ConfigProvider conf) {
        this.module = module;
        this.conf = conf;
    }

    public String ioModel() {
        return conf.get(SPARK_NETWORK_IO_MODE_KEY, "nio");
    }

    public int serverThreads() {
        return conf.getInt(SPARK_NETWORK_IO_SERVER_THREADS_KEY, 1);
    }

    public String getModuleName() {
        return module;
    }

    public int backLog() {
        return conf.getInt(SPARK_NETWORK_IO_BACKLOG_KEY, 50);
    }

    public int sendBuf() {
        return conf.getInt(SPARK_NETWORK_IO_SEND_BUFFER_KEY, -1);
    }

    public int receiveBuf() {
        return conf.getInt(SPARK_NETWORK_IO_RECEIVE_BUFFER_KEY, -1);
    }

    public int numConnectionsPerPeer() {
        return conf.getInt(SPARK_NETWORK_IO_NUM_CONNECTIONS_PER_PEER_KEY, 10);
    }

    public int clientThreads() {
        return conf.getInt(SPARK_NETWORK_IO_CLIENT_THREADS_KEY, 10);
    }

    public boolean saslServerAlwaysEncrypt() {
        return conf.getBoolean("spark.network.sasl.serverAlwaysEncrypt", false);
    }

    public int maxIORetries() {
        return conf.getInt(SPARK_NETWORK_IO_MAXRETRIES_KEY, 3);
    }

    public int ioRetryWaitTimeMs() {
        return (int) timeStringAsSec(conf.get(SPARK_NETWORK_IO_RETRYWAIT_KEY, "5s")) * 1000;
    }

    public int memoryMapBytes() {
        return Ints.checkedCast(JavaUtils.byteStringAsBytes(
                conf.get("spark.storage.memoryMapThreshold", "2m")));
    }

    public boolean lazyFileDescriptor() {
        return conf.getBoolean(SPARK_NETWORK_IO_LAZYFD_KEY, true);
    }

    /**
     * The max number of chunks allowed to be transferred at the same time on shuffle service.
     * Note that new incoming connections will be closed when the max number is hit. The client will
     * retry according to the shuffle retry configs (see `spark.shuffle.io.maxRetries` and
     * `spark.shuffle.io.retryWait`), if those limits are reached the task will fail with fetch
     * failure.
     */
    public long maxChunksBeingTransferred() {
        String maxChunksBeingTransferred = conf.get("spark.shuffle.maxChunksBeingTransferred");
        return NumberUtils.toLong(maxChunksBeingTransferred, Long.MAX_VALUE);
    }

    public int connectionTimeoutMs() {
        long defaultNetworkTimeoutS = timeStringAsSec(conf.get("spark.network.timeout", "120s"));
        long defaultTimeoutMs = timeStringAsSec(conf.get("io.connectionTimeout", defaultNetworkTimeoutS + "s")) * 1000;
        return (int) defaultTimeoutMs;
    }
}
