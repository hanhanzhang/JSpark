package com.sdu.spark.rpc.netty;

import com.sdu.spark.rpc.RpcAddress;
import lombok.AllArgsConstructor;
import lombok.Getter;

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
    @AllArgsConstructor
    @Getter
    class RemoteProcessConnect implements IndexMessage {
        private RpcAddress address;
    }

    /**
     * RpcEnv远端客户断开连接消息
     * */
    @AllArgsConstructor
    @Getter
    class RemoteProcessDisconnect implements IndexMessage {
        private RpcAddress address;
    }

    /**
     * RpcEnv远端交互异常消息
     * */
    @AllArgsConstructor
    @Getter
    class RemoteProcessConnectionError implements IndexMessage {
        private Throwable cause;
        private RpcAddress address;
    }

    /**
     * RpcEnv发送给远端的消息
     * */
    @AllArgsConstructor
    @Getter
    class RpcMessage implements IndexMessage {
        private RpcAddress senderAddress;
        private Object content;
        private NettyRpcCallContext context;
    }

    @AllArgsConstructor
    @Getter
    class OneWayMessage implements IndexMessage {
        private RpcAddress senderAddress;
        private Object content;
    }
}
