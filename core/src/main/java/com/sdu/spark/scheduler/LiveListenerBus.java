package com.sdu.spark.scheduler;

import com.sdu.spark.rpc.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author hanhan.zhang
 * */
public class LiveListenerBus implements SparkListenerBus {

    private static final Logger LOGGER = LoggerFactory.getLogger(LiveListenerBus.class);
    private static final String NAME = "SparkListenerBus";

    public SparkConf conf;

    /******************************Spark Event********************************/
    private AtomicBoolean stopped = new AtomicBoolean(false);
    // 丢弃事件数
    private AtomicLong droppedEventsCounter = new AtomicLong(0L);
    private AtomicBoolean logDroppedEvent = new AtomicBoolean(false);
    // 事件队列
    private LinkedBlockingQueue<SparkListenerEvent> eventQueue;
    // 并发控制
    private Semaphore eventLock = new Semaphore(0);
    private volatile boolean processingEvent = false;
    // 事件处理线程
    private Thread listenerThread = new Thread(NAME) {
        @Override
        public void run() {
            while (true) {
                try {
                    eventLock.acquire();
                    processingEvent = true;
                    SparkListenerEvent event = eventQueue.poll();
                    if (event != null) {
                        postToAll(event);
                    }
                } catch (InterruptedException e) {
                    // ignore
                } finally {
                    processingEvent = false;
                }
            }
        }
    };


    public LiveListenerBus(SparkConf conf) {
        this.conf = conf;
        eventQueue = new LinkedBlockingQueue<>(conf.getInt("spark.scheduler.listenerbus.eventqueue.capacity", 1024));
    }

    public void post(SparkListenerEvent event) {
        if (stopped.get()) {
            LOGGER.info("{}已停止, 丢弃事件: {}", NAME, event);
            return;
        }

        boolean add = eventQueue.offer(event);
        if (add) {

        } else {
            onDropEvent(event);
        }
    }

    private void onDropEvent(SparkListenerEvent event) {
        droppedEventsCounter.incrementAndGet();
        if (logDroppedEvent.compareAndSet(false, true)) {
            // Only log the following message once to avoid duplicated annoying logs.
            LOGGER.info("Dropping SparkListenerEvent because no remaining room in event queue. " +
                    "This likely means one of the SparkListeners is too slow and cannot keep up with " +
                    "the rate at which tasks are being started by the scheduler.");
        }
    }
}
