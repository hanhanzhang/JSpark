package com.sdu.spark.rpc.netty;

import com.sdu.spark.rpc.LocalResponseCallback;

import java.util.concurrent.*;

/**
 * @author hanhan.zhang
 * */
public class NettyLocalResponseCallback<T> implements LocalResponseCallback<T> {

    private CountDownLatch finished = new CountDownLatch(1);

    private T value;

    private Throwable error;

    @Override
    public void onSuccess(T value) {
        this.value = value;
        finished.countDown();
    }

    @Override
    public void onFailure(Throwable cause) {
        this.error = cause;
        finished.countDown();
    }

    public T getResponse() throws ExecutionException, InterruptedException {
        finished.await();
        if (error != null) {
            throw new ExecutionException("task execute exception", error);
        }
        return value;
    }
}
