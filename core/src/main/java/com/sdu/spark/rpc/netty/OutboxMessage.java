package com.sdu.spark.rpc.netty;

import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public interface OutboxMessage {

    void sendWith(TransportClient client);

    void onFailure(Throwable e);

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
}
