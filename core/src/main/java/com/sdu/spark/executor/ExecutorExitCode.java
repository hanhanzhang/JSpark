package com.sdu.spark.executor;

import java.io.Serializable;

import static com.sdu.spark.utils.SparkExitCode.OOM;
import static com.sdu.spark.utils.SparkExitCode.UNCAUGHT_EXCEPTION;
import static com.sdu.spark.utils.SparkExitCode.UNCAUGHT_EXCEPTION_TWICE;

/**
 * @author hanhan.zhang
 * */
public class ExecutorExitCode {

    /** DiskStore failed to create a local temporary directory after many attempts. */
    public final static int DISK_STORE_FAILED_TO_CREATE_DIR = 53;

    /** ExternalBlockStore failed to initialize after many attempts. */
    public final static int EXTERNAL_BLOCK_STORE_FAILED_TO_INITIALIZE = 54;

    /** ExternalBlockStore failed to create a local temporary directory after many attempts. */
    public final static int EXTERNAL_BLOCK_STORE_FAILED_TO_CREATE_DIR = 55;

    /**
     * Executor is unable to send heartbeats to the driver more than
     * "spark.executor.heartbeat.maxFailures" times.
     */
    public final static int HEARTBEAT_FAILURE = 56;


    private static String explainExitCode(int exitCode) {
        switch (exitCode) {
            case UNCAUGHT_EXCEPTION:
                return "Uncaught exception";
            case UNCAUGHT_EXCEPTION_TWICE:
                return "Uncaught exception, and logging the exception failed";
            case OOM:
                return "OutOfMemoryError";
            case DISK_STORE_FAILED_TO_CREATE_DIR:
                return "Failed to create local directory (bad spark.local.dir?)";
                // TODO: replace external block store with concrete implementation name
            case EXTERNAL_BLOCK_STORE_FAILED_TO_INITIALIZE:
                return "ExternalBlockStore failed to initialize.";
                // TODO: replace external block store with concrete implementation name
            case EXTERNAL_BLOCK_STORE_FAILED_TO_CREATE_DIR:
                return "ExternalBlockStore failed to create a local temporary directory.";
            case HEARTBEAT_FAILURE:
                return "Unable to send heartbeats to driver.";
            default:
                if (exitCode > 128) {
                    return String.format("Unknown executor exit code (%d) died from signal %d ?", exitCode, exitCode - 128);
                }
                return String.format("Unknown executor exit code (%d)", exitCode);
        }
    }

    public static class ExecutorLossReason implements Serializable {
        private String message;

        public ExecutorLossReason(String message) {
            this.message = message;
        }

        @Override
        public String toString() {
            return message;
        }
    }

    public static class ExecutorExited extends ExecutorLossReason {
        public int exitCode;
        public boolean exitCausedByApp;

        public ExecutorExited(int exitCode, boolean exitCausedByApp, String reason) {
            super(reason);
            this.exitCode = exitCode;
            this.exitCausedByApp = exitCausedByApp;
        }

        public static ExecutorExited apply(int exitCode, boolean exitCausedByApp) {
            return new ExecutorExited(
                    exitCode,
                    exitCausedByApp,
                    ExecutorExitCode.explainExitCode(exitCode));
        }
    }

    public static class ExecutorKilled extends ExecutorLossReason {
        public ExecutorKilled() {
            super("Executor killed by driver.");
        }
    }

    public static class LossReasonPending extends ExecutorLossReason {
        public LossReasonPending() {
            super("Pending loss reason.");
        }
    }

    public static class SlaveLost extends ExecutorLossReason {
        public boolean workerLost = false;

        public SlaveLost() {
            this("Slave lost", false);
        }

        public SlaveLost(String message) {
            this(message, false);
        }

        public SlaveLost(boolean workerLost) {
            this("Slave lost", workerLost);
        }

        public SlaveLost(String message, boolean workerLost) {
            super(message);
            this.workerLost = workerLost;
        }
    }

}
