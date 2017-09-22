package com.sdu.spark.network.protocol;

import com.sdu.spark.network.buffer.ManagedBuffer;

/**
 * @author hanhan.zhang
 * */
public abstract class AbstractMessage implements Message {
    /**
     * 消息体
     * */
    private final ManagedBuffer body;
    private final boolean isBodyInFrame;


    protected AbstractMessage() {
        this(null, false);
    }

    protected AbstractMessage(ManagedBuffer body, boolean isBodyInFrame) {
        this.body = body;
        this.isBodyInFrame = isBodyInFrame;
    }

    @Override
    public ManagedBuffer body() {
        return body;
    }

    @Override
    public boolean isBodyInFrame() {
        return isBodyInFrame;
    }
}
