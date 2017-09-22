package com.sdu.spark.network.client;

import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public interface RpcResponseCallback {

    void onSuccess(ByteBuffer response);

    void onFailure(Throwable e);
}
