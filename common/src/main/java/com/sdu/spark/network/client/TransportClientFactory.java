package com.sdu.spark.network.client;

import com.sdu.spark.network.TransportContext;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
public class TransportClientFactory {

    private TransportContext context;
    private List<TransportClientBootstrap> clientBootstraps;

    /**
     * 客户端连接池
     * */
    private static class ClientPool {

    }

    public TransportClientFactory(TransportContext context, List<TransportClientBootstrap> clientBootstraps) {
        this.context = context;
        this.clientBootstraps = clientBootstraps;
    }

    public TransportClient createClient(String remoteHost, int remotePort) {
        return null;
    }
}
