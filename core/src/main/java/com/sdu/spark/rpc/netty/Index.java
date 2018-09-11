package com.sdu.spark.rpc.netty;


import com.sdu.spark.rpc.RpcEndpoint;
import com.sdu.spark.rpc.RpcEndpointRef;
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

    private RpcEndpoint endPoint;
    private RpcEndpointRef endPointRef;
    private LinkedList<IndexMessage> messageBox = new LinkedList<>();

    // 并发控制
    private boolean enableConcurrent = false;
    private int numActiveThreads = 0;

    private volatile boolean stopped = false;

    public Index(RpcEndpoint endPoint, RpcEndpointRef endPointRef) {
        this.endPoint = endPoint;
        this.endPointRef = endPointRef;
        messageBox.add(new OnStart());
    }

    /** 投递消息 */
    public synchronized void post(IndexMessage message) {
        if (stopped) {
            onDrop(message);
        } else {
            messageBox.add(message);
        }
    }

    /** 处理消息 */
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
            safelyCall(endPoint, message, new IndexMessageHandler(dispatcher));
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

    private void safelyCall(RpcEndpoint endPoint, IndexMessage message, MessageHandler action) {
        try {
            action.handle(message);
        } catch (Exception e) {
            endPoint.onError(e);
        }
    }

    private interface MessageHandler {
        void handle(IndexMessage message);
    }

    private class IndexMessageHandler implements MessageHandler {

        private Dispatcher dispatcher;

        IndexMessageHandler(Dispatcher dispatcher) {
            this.dispatcher = dispatcher;
        }

        @Override
        public void handle(IndexMessage message) {
            if (message instanceof OnStart) {                        // 信箱启动
                endPoint.onStart();
                // ThreadSafeRpcEndpoint线程安全, 逐条处理消息
                if (!(endPoint instanceof ThreadSafeRpcEndpoint)) {
                    synchronized (this) {
                        if (!stopped) {
                            enableConcurrent = true;
                        }
                    }
                }
            } else if (message instanceof OnStop) {                   // 信箱关闭
                int activeThreads;
                synchronized (this) {
                    activeThreads = numActiveThreads;
                }
                assert activeThreads == 1 :
                        String.format("There should be only a single active thread but found %d threads.", activeThreads);
                dispatcher.removeRpcEndPointRef(endPoint);
                endPoint.onStop();
                assert isEmpty() : "OnStop should be the last message";
            } else if (message instanceof RpcMessage) {                 // 向远端发送消息
                RpcMessage rpcMessage = (RpcMessage) message;
                endPoint.receiveAndReply(rpcMessage.content, rpcMessage.context);
            } else if (message instanceof RemoteProcessConnect) {       // 远端连接到RpcEnv[广播给每个RpcEndPoint]
                endPoint.onConnected(((RemoteProcessConnect) message).address);
            } else if (message instanceof RemoteProcessDisconnected) {    // 远端关闭RpcEnv连接[广播给每个RpcEndPoint]
                endPoint.onDisconnected(((RemoteProcessDisconnected) message).remoteAddress);
            } else if (message instanceof OneWayMessage) {
                OneWayMessage oneWayMessage = (OneWayMessage) message;
                endPoint.receive(oneWayMessage.content);
            } else if (message instanceof RemoteProcessConnectionError) {
                RemoteProcessConnectionError connectionError = (RemoteProcessConnectionError) message;
                endPoint.onNetworkError(connectionError.cause, connectionError.address);
            }
        }
    }
}
