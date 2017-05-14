package com.sdu.spark.network.protocol;

import com.sdu.spark.network.buffer.ManagedBuffer;

/**
 * @author hanhan.zhang
 * */
public abstract class AbstractResponseMessage extends AbstractMessage implements ResponseMessage {

    protected AbstractResponseMessage(ManagedBuffer body, boolean isBodyInFrame) {
        super(body, isBodyInFrame);
    }

    public abstract ResponseMessage createFailureResponse(String error);
}
