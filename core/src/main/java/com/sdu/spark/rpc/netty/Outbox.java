package com.sdu.spark.rpc.netty;


import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.rpc.RpcAddress;

import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 发送信箱, 线程安全
 *
 * @author hanhan.zhang
 * */
public class Outbox {

    private NettyRpcEnv rpcEnv;

    /**
     * 远端服务地址
     * */
    public RpcAddress address;

    /**
     * 远端服务客户端
     * */
    private TransportClient client;

    private LinkedBlockingQueue<OutboxMessage> messages = new LinkedBlockingQueue<>();

    private boolean stopped = false;

    public Outbox(NettyRpcEnv rpcEnv, RpcAddress address) {
        this.rpcEnv = rpcEnv;
        this.address = address;
    }

    public void send(OutboxMessage message) {
        if (stopped) {
            message.onFailure(new IllegalStateException("Message is dropped because Outbox is stopped"));
            return;
        }
        synchronized (this) {
            messages.add(message);
        }
        drainOutbox();
    }

    /**
     * 发送消息
     * */
    private void drainOutbox() {
        OutboxMessage message = null;
        synchronized (this) {
            if (stopped) {
                return;
            }
            if (client == null) {
                launchConnectTask();
                return;
            }
            message = messages.poll();
            if (message == null) {
                return;
            }
        }

        while (true) {
            message.sendWith(client);
            synchronized (this) {
                message = messages.poll();
                if (message == null) {
                    return;
                }
            }
        }

    }

    private void launchConnectTask() {
        Future<TransportClient> future = rpcEnv.asyncCreateClient(address);
        try {
            TransportClient transportClient = future.get();
            synchronized (this) {
                client = transportClient;
                if (stopped) {
                    closeClient();
                }
            }
            drainOutbox();
        } catch (Exception e) {
            // ignore
        }
    }

    private void closeClient() {
        client = null;
    }

    public void stop() {
        synchronized (this) {
            if (stopped) {
                return;
            }
            stopped = true;
            closeClient();
        }
        OutboxMessage message = messages.poll();
        while (message != null) {
            message.onFailure(new IllegalStateException("Message is dropped because Outbox is stopped"));
            message = messages.poll();
        }
    }

}
