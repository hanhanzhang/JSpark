package com.sdu.spark.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * @author hanhan.zhang
 * */
public class ShutdownHookManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShutdownHookManager.class);

    private static final int DEFAULT_SHUTDOWN_PRIORITY = 100;

    private static final ShutdownHookManager MGR = new ShutdownHookManager();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                MGR.shutdownInProgress.set(true);
                synchronized (MGR.hooks) {
                    SparkShutdownHook hook = MGR.hooks.poll();
                    while (hook != null) {
                        try {
                            hook.run();
                        } catch (Exception e) {
                            LOGGER.error("shutdown hook failure", e);
                        }
                        hook = MGR.hooks.poll();
                    }
                }
            }
        });
    }

    private final PriorityQueue<SparkShutdownHook> hooks = new PriorityQueue<>();
    private AtomicBoolean shutdownInProgress = new AtomicBoolean(false);

    private ShutdownHookManager() {}

    public SparkShutdownHook add(ShutdownHook hook) {
        return add(DEFAULT_SHUTDOWN_PRIORITY, hook);
    }

    public SparkShutdownHook add(int priority, ShutdownHook hook) {
        synchronized (hooks) {
            if (!shutdownInProgress.get()) {
                throw new IllegalStateException("Shutdown hooks cannot be modified during shutdown.");
            }

            SparkShutdownHook shutdownHook = new SparkShutdownHook(priority, hook);
            hooks.add(shutdownHook);
            return shutdownHook;
        }
    }

    public boolean removeShutdownHook(SparkShutdownHook shutdownHook) {
        synchronized (hooks) {
            return hooks.remove(shutdownHook);
        }
    }

    public static boolean inShutdown() {
        try {
            Thread hook = new Thread() {
                @Override
                public void run() {
                    super.run();
                }
            };
            Runtime.getRuntime().addShutdownHook(hook);
            Runtime.getRuntime().removeShutdownHook(hook);
            return false;
        } catch (IllegalStateException e) {
            return true;
        }
    }

    public static ShutdownHookManager get() {
        return MGR;
    }

    public class SparkShutdownHook implements Comparable<SparkShutdownHook> {

        private int priority;
        private ShutdownHook hook;

        public SparkShutdownHook(int priority, ShutdownHook hook) {
            this.priority = priority;
            this.hook = hook;
        }

        public void run() {
            this.hook.shutdown();
        }

        @Override
        public int compareTo(SparkShutdownHook o) {
            return o.priority - priority;
        }
    }

    public interface ShutdownHook {
        void shutdown();
    }
}
