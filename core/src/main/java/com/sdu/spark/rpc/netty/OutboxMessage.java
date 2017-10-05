package com.sdu.spark.rpc.netty;

import com.google.common.util.concurrent.SettableFuture;
import com.sdu.spark.network.client.TransportClient;
import com.sdu.spark.network.client.RpcResponseCallback;
import com.sdu.spark.rpc.RpcEnvStoppedException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        private static final Logger LOGGER = LoggerFactory.getLogger(OneWayOutboxMessage.class);

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
            if (e instanceof RpcEnvStoppedException) {
                LOGGER.debug(e.getMessage());
            } else {
                LOGGER.warn("Failed to send one-way RPC.", e);
            }
        }
    }

    /**
     *
     * */
    class CheckExistence implements OutboxMessage {

        public String name;

        public CheckExistence(String name) {
            this.name = name;
        }

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

        private RpcResponseCallback callback;

        public RpcOutboxMessage(ByteBuffer content, RpcResponseCallback callback) {
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
            callback.onSuccess(response);
        }

        @Override
        public void onFailure(Throwable e) {
            callback.onFailure(e);
        }
    }
}
