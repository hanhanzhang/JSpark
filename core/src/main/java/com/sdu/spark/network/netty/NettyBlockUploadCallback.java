package com.sdu.spark.network.netty;

import com.sdu.spark.network.client.RpcResponseCallback;

import java.nio.ByteBuffer;
import java.util.concurrent.*;

/**
 * @author hanhan.zhang
 * */
public class NettyBlockUploadCallback implements RpcResponseCallback {


    private boolean result = false;
    private Throwable error;
    private boolean cancelled = false;
    private CountDownLatch countDownLatch = new CountDownLatch(1);

    @Override
    public void onSuccess(ByteBuffer response) {
        result = true;
    }

    @Override
    public void onFailure(Throwable e) {
        this.error = e;
    }

    public Future<Boolean> getResponseFuture() {
        return new Future<Boolean>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                if (countDownLatch.getCount() > 1) {
                    cancelled = true;
                    countDownLatch.countDown();
                    return true;
                }
                return false;
            }

            @Override
            public boolean isCancelled() {
                return cancelled;
            }

            @Override
            public boolean isDone() {
                return countDownLatch.getCount() == 0;
            }

            @Override
            public Boolean get() throws InterruptedException, ExecutionException {
                countDownLatch.await();
                return result;
            }

            @Override
            public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                countDownLatch.await(timeout, unit);
                return result;
            }
        };
    }
}
