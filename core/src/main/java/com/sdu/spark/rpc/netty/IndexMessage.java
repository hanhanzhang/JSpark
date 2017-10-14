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

        @Override
        public String toString() {
            return String.format("remote %s process connecting message", address);
        }
    }

    class RemoteProcessConnected implements IndexMessage {
        public RpcAddress remoteAddress;

        public RemoteProcessConnected(RpcAddress remoteAddress) {
            this.remoteAddress = remoteAddress;
        }

        @Override
        public String toString() {
            return String.format("remote %s process connected message", remoteAddress);
        }
    }

    class RemoteProcessDisconnected implements IndexMessage {
        public RpcAddress remoteAddress;

        public RemoteProcessDisconnected(RpcAddress remoteAddress) {
            this.remoteAddress = remoteAddress;
        }

        @Override
        public String toString() {
            return String.format("remote %s process disconnected message", remoteAddress);
        }
    }

    class RemoteProcessDisconnect implements IndexMessage {
        public RpcAddress address;

        public RemoteProcessDisconnect(RpcAddress address) {
            this.address = address;
        }

        @Override
        public String toString() {
            return String.format("remote %s process disconnect message", address);
        }
    }

    class RemoteProcessConnectionError implements IndexMessage {
        public Throwable cause;
        public RpcAddress address;

        public RemoteProcessConnectionError(Throwable cause, RpcAddress address) {
            this.cause = cause;
            this.address = address;
        }

        @Override
        public String toString() {
            return String.format("remote %s process connection error message", address);
        }
    }

    class RpcMessage implements IndexMessage {
        public RpcAddress senderAddress;
        public Object content;
        public NettyRpcCallContext context;

        public RpcMessage(RpcAddress senderAddress, Object content, NettyRpcCallContext context) {
            this.senderAddress = senderAddress;
            this.content = content;
            this.context = context;
        }

        @Override
        public String toString() {
            return String.format("rpc message from %s ", senderAddress);
        }
    }

    class OneWayMessage implements IndexMessage {
        public RpcAddress senderAddress;
        public Object content;

        public OneWayMessage(RpcAddress senderAddress, Object content) {
            this.senderAddress = senderAddress;
            this.content = content;
        }

        @Override
        public String toString() {
            return String.format("rpc one way message from %s ", senderAddress);
        }
    }
}
