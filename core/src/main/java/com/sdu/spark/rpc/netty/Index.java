package com.sdu.spark.rpc.netty;


import com.sdu.spark.rpc.RpcEndPoint;
import com.sdu.spark.rpc.RpcEndPointRef;

import java.util.LinkedList;

import static com.sdu.spark.rpc.netty.IndexMessage.*;

/**
 * 接收信箱
 *
 * @author hanhan.zhang
 * */
public class Index {
    /**
     * 消息发送方
     * */
    private RpcEndPoint endPoint;
    /**
     * 消息接收方
     * */
    private RpcEndPointRef endPointRef;

    /**
     * 消息信箱
     * */
    private LinkedList<IndexMessage> messageBox = new LinkedList<>();

    private boolean stopped = false;

    public Index(RpcEndPoint endPoint, RpcEndPointRef endPointRef) {
        this.endPoint = endPoint;
        this.endPointRef = endPointRef;
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
        IndexMessage message = null;
        synchronized (this) {
            message = messageBox.poll();
        }

        while (true) {
            if (message instanceof OnStart) {
                endPoint.onStart();
            } else if (message instanceof OnStop) {
                dispatcher.removeRpcEndPointRef(endPoint);
                endPoint.onStop();
            } else if (message instanceof RemoteProcessConnect) {
                endPoint.onConnect(((RemoteProcessConnect) message).getAddress());
            } else if (message instanceof RemoteProcessDisconnect) {
                endPoint.onDisconnect(((RemoteProcessDisconnect) message).getAddress());
            } else if (message instanceof RpcMessage) {

            }
            message = messageBox.poll();
            if (message == null) {
                return;
            }
        }
    }

    private void onDrop(IndexMessage message) {

    }

    public synchronized void stop() {
        if (!stopped) {
            stopped = true;
            messageBox.add(new OnStop());
        }
    }
}
