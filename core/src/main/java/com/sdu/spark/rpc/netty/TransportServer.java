package com.sdu.spark.rpc.netty;

/**
 * @author hanhan.zhang
 * */
public class TransportServer {

    /**
     * Server端绑定的端口
     * */
    private int port = -1;

    public int getPort() {
        if (port == -1) {
            throw new IllegalStateException("Server not initialized");
        }
        return port;
    }
}
