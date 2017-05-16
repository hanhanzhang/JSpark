package com.sdu.spark.rpc.netty;

import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.rpc.ICallback;
import com.sdu.spark.network.client.RpcResponseCallback;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public interface OutboxMessage extends Serializable {

    void sendWith(TransportClient client);

    void onFailure(Throwable e);

    /**
     * 单向网络消息
     * */
    class OneWayOutboxMessage implements OutboxMessage {

        private ByteBuffer content;

        public OneWayOutboxMessage(ByteBuffer content) {
            this.content = content;
        }

        @Override
        public void sendWith(TransportClient client) {
            client.send(content);
        }

        @Override
        public void onFailure(Throwable e) {
            //ignore
        }
    }

    /**
     *
     * */
    @AllArgsConstructor
    @Getter
    class CheckExistence implements OutboxMessage {
        private String name;

        @Override
        public void sendWith(TransportClient client) {

        }

        @Override
        public void onFailure(Throwable e) {

        }
    }

    class RpcOutboxMessage implements OutboxMessage, RpcResponseCallback {
        private ByteBuffer content;
        private TransportClient client;
        private long requestId;

        public RpcOutboxMessage(ByteBuffer content, ICallback<TransportClient, Void> callback) {
            this.content = content;
        }

        @Override
        public void sendWith(TransportClient client) {
            this.client = client;
            requestId = this.client.sendRpc(content, this);
        }

        @Override
        public void onSuccess(ByteBuffer response) {

        }

        @Override
        public void onFailure(Throwable e) {

        }

        public void onTimeout() {
            if (client != null) {
                client.removeRpcRequest(this.requestId);
            }
        }
    }
}
