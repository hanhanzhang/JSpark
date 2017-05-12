package com.sdu.spark.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.*;

/**
 * @author hanhan.zhang
 * */
public class ThreadUtils {

    private static ThreadFactory namedThreadFactory(String prefix, boolean daemon) {
        return new ThreadFactoryBuilder().setDaemon(daemon).setNameFormat(prefix).build();
    }

    public static ThreadPoolExecutor newDaemonCachedThreadPool(String prefix, int maxThreadNumber, int keepAliveSeconds) {
        ThreadFactory threadFactory = namedThreadFactory(prefix, true);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(maxThreadNumber, maxThreadNumber, keepAliveSeconds,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>(), threadFactory);
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    public static ScheduledExecutorService newDaemonSingleThreadScheduledExecutor(String threadName) {
        ThreadFactory threadFactory = namedThreadFactory(threadName, true);
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, threadFactory);
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

}
