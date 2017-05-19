package com.sdu.spark.rpc;

import com.sdu.spark.network.client.RpcResponseCallback;

import java.nio.ByteBuffer;
import java.util.concurrent.*;

/**
 * @author hanhan.zhang
 * */
public class NettyRpcResponseCallback implements RpcResponseCallback {

    private CountDownLatch finished = new CountDownLatch(1);

    private boolean cancelled = false;

    private ByteBuffer value;

    private Throwable cause;

    @Override
    public void onSuccess(ByteBuffer response) {
        this.value = response;
        finished.countDown();
    }

    @Override
    public void onFailure(Throwable e) {
        this.cause = e;
        finished.countDown();
    }

    public Future<ByteBuffer> getResponseFuture() {
        return new Future<ByteBuffer>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                if (finished.getCount() > 1) {
                    cancelled = true;
                    finished.countDown();
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
                return finished.getCount() == 0;
            }

            @Override
            public ByteBuffer get() throws InterruptedException, ExecutionException {
                finished.await();
                return getValue();
            }

            @Override
            public ByteBuffer get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                if (finished.await(timeout, unit)) {
                    return getValue();
                } else {
                    throw new TimeoutException("get result after waiting " + timeout + ", unit = " + unit);
                }
            }

            private ByteBuffer getValue() throws ExecutionException {
                if (cancelled) {
                    throw new CancellationException("task already cancel");
                } else if (cause != null) {
                    throw new ExecutionException("task execute exception", cause);
                }
                return value;
            }

        };
    }
}
