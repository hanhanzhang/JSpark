package com.sdu.spark.utils;

/**
 * @author hanhan.zhang
 * */
public class SparkExitCode {

    /** The default uncaught exception handler was reached. */
    public static final int UNCAUGHT_EXCEPTION = 50;

    /** The default uncaught exception handler was called and an exception was encountered while
     logging the exception. */
    public static final int UNCAUGHT_EXCEPTION_TWICE = 51;

    /** The default uncaught exception handler was reached, and the uncaught exception was an
     OutOfMemoryError. */
    public static final int OOM = 52;

}
