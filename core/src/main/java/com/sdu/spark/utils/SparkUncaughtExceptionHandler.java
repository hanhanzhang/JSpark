package com.sdu.spark.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.sdu.spark.utils.ShutdownHookManager.inShutdown;
import static com.sdu.spark.utils.SparkExitCode.OOM;
import static com.sdu.spark.utils.SparkExitCode.UNCAUGHT_EXCEPTION;

/**
 * 线程异常处理
 *
 * @author hanhan.zhang
 * */
public class SparkUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkUncaughtExceptionHandler.class);

    private boolean exitOnUncaughtException;

    public SparkUncaughtExceptionHandler() {
        this(true);
    }

    public SparkUncaughtExceptionHandler(boolean exitOnUncaughtException) {
        this.exitOnUncaughtException = exitOnUncaughtException;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        String inShutdownMsg = inShutdown() ? "[Container in shutdown] " : "";

        String errMsg = "Uncaught exception in thread ";
        LOGGER.error(inShutdownMsg + errMsg + t, e);

        // We may have been called from a shutdown hook. If so, we must not call System.exit().
        // (If we do, we will deadlock.)
        if (!inShutdown()) {
            if (e instanceof OutOfMemoryError) {
                System.exit(OOM);
            } else if (exitOnUncaughtException) {
                System.exit(UNCAUGHT_EXCEPTION);
            }
        }
    }

}
