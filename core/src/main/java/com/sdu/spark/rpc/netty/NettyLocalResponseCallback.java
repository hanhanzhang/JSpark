package com.sdu.spark.rpc.netty;

import com.sdu.spark.rpc.LocalResponseCallback;

import java.util.concurrent.*;

/**
 * @author hanhan.zhang
 * */
public class NettyLocalResponseCallback<T> implements LocalResponseCallback<T> {

    private CountDownLatch finised = new CountDownLatch(1);

    private T value;

    private Throwable error;

    @Override
    public void onSuccess(T value) {
        this.value = value;
        finised.countDown();
    }

    @Override
    public void onFailure(Throwable cause) {
        this.error = cause;
        finised.countDown();
    }

    public T getResponse() throws ExecutionException, InterruptedException {
        finised.await();
        if (error != null) {
            throw new ExecutionException("task execute exception", error);
        }
        return value;
    }
}
