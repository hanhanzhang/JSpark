package com.sdu.spark.utils;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * @author hanhan.zhang
 * */
public class ShutdownHookManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShutdownHookManager.class);

    private static final int DEFAULT_SHUTDOWN_PRIORITY = 100;
    public static final int TEMP_DIR_SHUTDOWN_PRIORITY = 25;

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

    private final Set<String> shutdownDeletePaths = Sets.newHashSet();

    private ShutdownHookManager() {
        add(TEMP_DIR_SHUTDOWN_PRIORITY, () -> {
            LOGGER.info("Shutdown hook called");
            shutdownDeletePaths.forEach(file -> {
                try {
                    LOGGER.info("Deleting directory {}", file);
                    Utils.deleteRecursively(new File(file));
                } catch (Exception e) {
                    LOGGER.error("Exception while deleting Spark temp dir {}", file, e);
                }
            });
        });
    }

    public SparkShutdownHook add(ShutdownHook hook) {
        return add(DEFAULT_SHUTDOWN_PRIORITY, hook);
    }

    public SparkShutdownHook add(int priority, ShutdownHook hook) {
        synchronized (hooks) {
            if (shutdownInProgress.get()) {
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

    public boolean hasRootAsShutdownDeleteDir(File file) {
        String path = file.getAbsolutePath();
        long num = 0L;
        synchronized (shutdownDeletePaths) {
            num = shutdownDeletePaths.stream()
                                     .filter(rootPath -> rootPath.equals(path) || rootPath.startsWith(path))
                                     .count();

        }
        if (num > 0) {
            LOGGER.info("path {} already present as root for deletion.", path);
        }
        return num > 0;
    }

    public void registerShutdownDeleteDir(File file) {
        synchronized (shutdownDeletePaths) {
            shutdownDeletePaths.add(file.getAbsolutePath());
        }
    }

    public void removeShutdownDeleteDir(File file) {
        synchronized (shutdownDeletePaths) {
            shutdownDeletePaths.remove(file.getAbsolutePath());
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
