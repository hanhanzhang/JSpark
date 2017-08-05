package com.sdu.spark.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author hanhan.zhang
 * */
public abstract class EventLoop<E> {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventLoop.class);

    private String name;
    private BlockingQueue<E> eventQueue = new LinkedBlockingQueue<>();

    private AtomicBoolean stopped = new AtomicBoolean(false);

    private Thread eventThread;

    public EventLoop(String name) {
        this.name = name;
        eventThread = new Thread(name) {
            @Override
            public void run() {
                try {
                    while (!stopped.get()) {
                        E event = eventQueue.take();
                        try {
                            onReceive(event);
                        } catch (Exception e) {
                            onError(e);
                            LOGGER.error("消息处理异常", e);
                        }
                    }
                } catch (InterruptedException e) {
                    LOGGER.error("获取消息异常", e);
                }
            }
        };
        eventThread.setDaemon(true);
    }

    public void start() {
        if (stopped.get()) {
            throw new IllegalStateException(name + " has already been stopped");
        }
        // Call onStart before starting the event thread to make sure it happens before onReceive
        onStart();
        eventThread.start();
    }

    public void post(E event) {
        try {
            eventQueue.put(event);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    public boolean isActive() {
        return eventThread.isAlive();
    }



    public void stop() {
        if (stopped.compareAndSet(false, true)) {
            eventThread.interrupt();
            try {
                eventThread.join();
                // Call onStop after the event thread exits to make sure onReceive happens before onStop
                onStop();
            } catch(InterruptedException e){
                Thread.currentThread().interrupt();
                onStop();
            }
        }
    }

    public void onStart() {}
    public void onStop() {}

    public abstract void onReceive(E event);

    public abstract void onError(Throwable cause);
}
