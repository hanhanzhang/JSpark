package com.sdu.spark.rpc.netty;


import com.sdu.spark.SparkException;
import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.rpc.RpcAddress;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.Future;

/**
 * RpcMessage发送信箱
 *
 * @author hanhan.zhang
 * */
public class Outbox {

    private NettyRpcEnv nettyEnv;

    /**远端服务地址*/
    public RpcAddress address;

    /**远端服务客户端*/
    private TransportClient client;

    private LinkedList<OutboxMessage> messages = new LinkedList<>();

    private boolean stopped = false;

    private Future<?> connectFuture = null;

    /**If there is any thread draining the message queue*/
    private boolean draining = false;

    public Outbox(NettyRpcEnv nettyEnv, RpcAddress address) {
        this.nettyEnv = nettyEnv;
        this.address = address;
    }

    public void send(OutboxMessage message) {
        boolean dropped;
        synchronized (this) {
            if (stopped) {
                dropped = true;
            } else {
                messages.add(message);
                dropped = false;
            }
        }
        if (dropped) {
            message.onFailure(new IllegalStateException("Message is dropped because Outbox is stopped"));
        } else {
            drainOutbox();
        }
    }

    /**
     * Drain the message queue. If there is other draining thread, just exit. If the connection has
     * not been established, launch a task in the `nettyEnv.clientConnectionExecutor` to setup the
     * connection.
     */
    private void drainOutbox() {
        OutboxMessage message;
        synchronized (this) {
            if (stopped) {
                return;
            }
            if (connectFuture != null) {
                // We are connecting to the remote address, so just exit
                return;
            }

            if (client == null) {
                // There is no connect task but client is null, so we need to launch the connect task.
                launchConnectTask();
                return;
            }

            if (draining) {
                // There is some thread draining, so just exit
                return;
            }

            message = messages.poll();
            if (message == null) {
                return;
            }
            draining = true;
        }

        // 消息处理
        while (true) {
            TransportClient _client = null;
            synchronized (this) {
                _client = this.client;
            }
            if (_client != null) {
                message.sendWith(_client);
            } else {
                assert stopped;
            }

            synchronized (this) {
                if (stopped) {
                    return;
                }
                message = messages.poll();
                if (message == null) {
                    draining = false;
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
            if (connectFuture != null) {
                connectFuture.cancel(true);
            }
            closeClient();
        }

        // We always check `stopped` before updating messages, so here we can make sure no thread will
        // update messages and it's safe to just drain the queue.
        OutboxMessage message = messages.poll();
        while (message != null) {
            message.onFailure(new SparkException("Message is dropped because Outbox is stopped"));
            message = messages.poll();
        }
    }

}
