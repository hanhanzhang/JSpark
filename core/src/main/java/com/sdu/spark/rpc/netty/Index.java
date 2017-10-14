package com.sdu.spark.rpc.netty;


import com.sdu.spark.rpc.RpcEndPoint;
import com.sdu.spark.rpc.RpcEndPointRef;
import com.sdu.spark.rpc.ThreadSafeRpcEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

import static com.sdu.spark.rpc.netty.IndexMessage.*;

/**
 * 接收信箱
 *
 * @author hanhan.zhang
 * */
public class Index {

    private static final Logger LOGGER = LoggerFactory.getLogger(Index.class);

    /**消息发送方*/
    private RpcEndPoint endPoint;
    /**消息接收方*/
    private RpcEndPointRef endPointRef;
    /**消息信箱*/
    private LinkedList<IndexMessage> messageBox = new LinkedList<>();
    private boolean enableConcurrent = false;
    private int numActiveThreads = 0;

    private boolean stopped = false;

    public Index(RpcEndPoint endPoint, RpcEndPointRef endPointRef) {
        this.endPoint = endPoint;
        this.endPointRef = endPointRef;
        /**投递启动消息, EndPoint调用OnStart方法*/
        messageBox.add(new OnStart());
    }

    /**
     * 投递消息
     * */
    public synchronized void post(IndexMessage message) {
        if (stopped) {
            onDrop(message);
        } else {
            messageBox.add(message);
        }
    }

    /**
     * 处理消息
     * */
    public void process(Dispatcher dispatcher) {
        IndexMessage message;
        synchronized (this) {
            // 当已有线程访问时, 退出
            if (!enableConcurrent && numActiveThreads != 0) {
                return;
            }
            message = messageBox.poll();
            if (message != null) {
                numActiveThreads += 1;
            } else {
                return;
            }
        }

        // 处理消息
        while (true) {
            safelyCall(endPoint, message, (msg) -> {
                if (msg instanceof OnStart) {                           // 信箱启动
                    endPoint.onStart();
                    // 允许多个线程访问, 进行消息处理
                    if (!(endPoint instanceof ThreadSafeRpcEndpoint)) {
                        synchronized (this) {
                            if (!stopped) {
                                enableConcurrent = true;
                            }
                        }
                    }
                } else if (msg instanceof OnStop) {                     // 信箱关闭
                    int activeThreads = 0;
                    synchronized (this) {
                        activeThreads = numActiveThreads;
                    }
                    assert activeThreads == 1 :
                            String.format("There should be only a single active thread but found %d threads.", activeThreads);
                    dispatcher.removeRpcEndPointRef(endPoint);
                    endPoint.onStop();
                    assert isEmpty() : "OnStop should be the last message";
                } else if (msg instanceof RpcMessage) {                 // 向远端发送消息
                    RpcMessage rpcMessage = (RpcMessage) msg;
                    endPoint.receiveAndReply(rpcMessage.content, rpcMessage.context);
                } else if (msg instanceof RemoteProcessConnect) {       // 远端连接到RpcEnv[广播给每个RpcEndPoint]
                    endPoint.onConnected(((RemoteProcessConnect) msg).address);
                } else if (msg instanceof RemoteProcessDisconnected) {    // 远端关闭RpcEnv连接[广播给每个RpcEndPoint]
                    endPoint.onDisconnected(((RemoteProcessDisconnected) msg).remoteAddress);
                } else if (msg instanceof OneWayMessage) {
                    OneWayMessage oneWayMessage = (OneWayMessage) msg;
                    endPoint.receive(oneWayMessage.content);
                } else if (msg instanceof RemoteProcessConnectionError) {
                    RemoteProcessConnectionError connectionError = (RemoteProcessConnectionError) msg;
                    endPoint.onNetworkError(connectionError.cause, connectionError.address);
                }
            });
            synchronized (this) {
                // 被当前线程访问, numActiveThreads至少等于1
                if (!enableConcurrent && numActiveThreads != 1) {
                    numActiveThreads -=1;
                    return;
                }
                message = messageBox.poll();
                if (message == null) {
                    numActiveThreads -=1;
                    return;
                }
            }

        }
    }

    private void onDrop(IndexMessage message) {
        LOGGER.warn("Drop {} because {} is stopped", message, endPointRef);
    }

    public synchronized void stop() {
        if (!stopped) {
            enableConcurrent = false;
            stopped = true;
            messageBox.add(new OnStop());
        }
    }

    private synchronized boolean isEmpty() {
       return messageBox.isEmpty();
    }

    private void safelyCall(RpcEndPoint endPoint, IndexMessage message, MessageHandler action) {
        try {
            action.handle(message);
        } catch (Exception e) {
            endPoint.onError(e);
        }
    }

    private interface MessageHandler {
        void handle(IndexMessage message);
    }
}
