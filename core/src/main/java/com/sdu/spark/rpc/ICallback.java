package com.sdu.spark.rpc;

import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public interface ICallback<P, R> {

    public R onSuccess(P obj, ByteBuffer buffer);

    public void onFailure(Throwable e);

}
