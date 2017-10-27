package com.sdu.spark.utils;

/**
 * @author hanhan.zhang
 * */
public class UninterruptibleThread extends Thread {

    private final Object uninterruptibleLock = new Object();
    private boolean uninterruptible = false;
    private boolean shouldInterruptThread = false;

    public UninterruptibleThread(String name) {
        this(null, name);
    }

    public UninterruptibleThread(Runnable target, String name) {
        super(target, name);
    }

    public <T> T runUninterruptibly(TargetInvoker<T> targetInvoker) {
        if (currentThread() != this) {
            throw new IllegalStateException(String.format("Call runUninterruptibly in a wrong thread " +
                    "Expected: %s but was %s", this, Thread.currentThread()));
        }

        synchronized (uninterruptibleLock) {
            if (uninterruptible) {
                return targetInvoker.invoke();
            }
        }

        synchronized (uninterruptibleLock) {
            // Clear the interrupted status if it's set.
            shouldInterruptThread = Thread.interrupted() || shouldInterruptThread;
            uninterruptible = true;
        }
        try {
            return targetInvoker.invoke();
        } finally {
            synchronized(uninterruptibleLock) {
                uninterruptible = false;
                if (shouldInterruptThread) {
                    // Recover the interrupted status
                    super.interrupt();
                    shouldInterruptThread = false;
                }
            }
        }
    }

    @Override
    public void interrupt() {
        synchronized(uninterruptibleLock){
            if (uninterruptible) {
                shouldInterruptThread = true;
            } else {
                super.interrupt();
            }
        }
    }

    public interface TargetInvoker<T> {
        T invoke();
    }
}
