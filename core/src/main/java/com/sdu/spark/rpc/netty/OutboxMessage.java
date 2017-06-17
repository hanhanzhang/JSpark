package com.sdu.spark.rpc.netty;

import com.sdu.spark.network.client.TransportClient;
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
    class CheckExistence implements OutboxMessage {

        public String name;

        @Override
        public void sendWith(TransportClient client) {

        }

        @Override
        public void onFailure(Throwable e) {

        }
    }

    class RpcOutboxMessage implements OutboxMessage, RpcResponseCallback {
        /**
         * RpcMessage内容
         * */
        public ByteBuffer content;
        /**
         * 消息发送客户端
         * */
        public TransportClient client;
        /**
         * 消息标识
         * */
        public long requestId;

        private NettyRpcResponseCallback callback;

        public RpcOutboxMessage(ByteBuffer content, NettyRpcResponseCallback callback) {
            this.content = content;
            this.callback = callback;
        }

        @Override
        public void sendWith(TransportClient client) {
            this.client = client;
            requestId = this.client.sendRpc(content, this);
        }

        @Override
        public void onSuccess(ByteBuffer response) {
            this.callback.onSuccess(response);
        }

        @Override
        public void onFailure(Throwable e) {
            this.callback.onFailure(e);
        }
    }
}
