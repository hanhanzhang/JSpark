package com.sdu.spark.rpc.netty;

import com.sdu.spark.rpc.RpcAddress;

/**
 * 信箱消息
 *
 * @author hanhan.zhang
 * */
public interface IndexMessage {

    /**
     * 启动信箱消息
     * */
    class OnStart implements IndexMessage {}

    /**
     * 关闭信箱消息
     * */
    class OnStop implements IndexMessage {}

    /**
     * RpcEnv远端客户连接信息
     * */
    class RemoteProcessConnect implements IndexMessage {
        public RpcAddress address;

        public RemoteProcessConnect(RpcAddress address) {
            this.address = address;
        }
    }

    class RemoteProcessConnected implements IndexMessage {
        public RpcAddress remoteAddress;

        public RemoteProcessConnected(RpcAddress remoteAddress) {
            this.remoteAddress = remoteAddress;
        }
    }

    class RemoteProcessDisconnected implements IndexMessage {
        public RpcAddress remoteAddress;

        public RemoteProcessDisconnected(RpcAddress remoteAddress) {
            this.remoteAddress = remoteAddress;
        }
    }

    /**
     * RpcEnv远端客户断开连接消息
     * */
    class RemoteProcessDisconnect implements IndexMessage {
        public RpcAddress address;

        public RemoteProcessDisconnect(RpcAddress address) {
            this.address = address;
        }
    }

    /**
     * RpcEnv远端交互异常消息
     * */
    class RemoteProcessConnectionError implements IndexMessage {
        public Throwable cause;
        public RpcAddress address;

        public RemoteProcessConnectionError(Throwable cause, RpcAddress address) {
            this.cause = cause;
            this.address = address;
        }
    }

    /**
     * RpcEnv发送给远端的消息
     * */
    class RpcMessage implements IndexMessage {
        public RpcAddress senderAddress;
        public Object content;
        public NettyRpcCallContext context;

        public RpcMessage(RpcAddress senderAddress, Object content, NettyRpcCallContext context) {
            this.senderAddress = senderAddress;
            this.content = content;
            this.context = context;
        }
    }

    class OneWayMessage implements IndexMessage {
        public RpcAddress senderAddress;
        public Object content;

        public OneWayMessage(RpcAddress senderAddress, Object content) {
            this.senderAddress = senderAddress;
            this.content = content;
        }
    }
}
