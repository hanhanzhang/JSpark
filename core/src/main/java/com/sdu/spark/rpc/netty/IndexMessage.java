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

    class OnStart implements IndexMessage {}

    class OnStop implements IndexMessage {}

    @AllArgsConstructor
    @Getter
    class RemoteProcessConnect implements IndexMessage {
        private RpcAddress address;
    }

    @AllArgsConstructor
    @Getter
    class RemoteProcessDisconnect implements IndexMessage {
        private RpcAddress address;
    }

    @AllArgsConstructor
    @Getter
    class RpcMessage implements IndexMessage {
        private RpcAddress senderAddress;
        private Object content;
    }

    @AllArgsConstructor
    @Getter
    class OneWayMessage implements IndexMessage {
        private RpcAddress senderAddress;
        private Object content;
    }
}
