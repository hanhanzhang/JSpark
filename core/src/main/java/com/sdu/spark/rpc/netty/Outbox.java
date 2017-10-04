package com.sdu.spark.rpc.netty;


import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.rpc.RpcAddress;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 发送信箱, 线程安全
 *
 * @author hanhan.zhang
 * */
public class Outbox {

    private NettyRpcEnv nettyEnv;

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

    private Future<?> connectFuture = null;

    public Outbox(NettyRpcEnv nettyEnv, RpcAddress address) {
        this.nettyEnv = nettyEnv;
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
            if (connectFuture != null) {
                // We are connecting to the remote address, so just exit
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
        connectFuture = nettyEnv.clientConnectionExecutor.submit(() -> {
           try {
               TransportClient client = nettyEnv.createClient(address);
               synchronized (this) {
                   this.client = client;
                   if (stopped) {
                       closeClient();
                   }
               }
           } catch (InterruptedException e) {
               // exit
           } catch (IOException e){
               synchronized (this) {
                   connectFuture = null;
               }
               handleNetworkFailure(e);
           }
            synchronized (this) {
                connectFuture = null;
            }
            // It's possible that no thread is draining now. If we don't drain here, we cannot send the
            // messages until the next message arrives.
            drainOutbox();
            return;
        });
    }

    private void handleNetworkFailure(Throwable e) {
        synchronized (this) {
            assert connectFuture == null;
            if (stopped) {
                return;
            }

            stopped = true;
            closeClient();
        }

        // 停止向address发送Rpc消息, 并通知
        nettyEnv.removeOutbox(address);
        OutboxMessage message = messages.poll();
        while (message != null) {
            message.onFailure(e);
            message = messages.poll();
        }

        assert messages.isEmpty();
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
