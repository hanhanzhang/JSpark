package com.sdu.spark.scheduler;

import com.sdu.spark.storage.BlockManagerId;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

import static java.lang.String.format;

/**
 * @author hanhan.zhang
 * */
public interface TaskEndReason extends Serializable {

    class Success implements TaskEndReason {}

    abstract class TaskFailedReason implements TaskEndReason {
        /**Error message displayed in the web UI*/
        public abstract String toErrorString();
        public boolean countTowardsTaskFailures() {
            return true;
        }
    }

    class ExceptionFailure extends TaskFailedReason {
        private String className;
        private String description;
        private List<StackTraceElement> stackTrace;
        private String fullStackTrace;
        private ThrowableSerializationWrapper exceptionWrapper;
        // TODO: Task Metric


        public ExceptionFailure(String className,
                                String description,
                                List<StackTraceElement> stackTrace,
                                String fullStackTrace,
                                ThrowableSerializationWrapper exceptionWrapper) {
            this.className = className;
            this.description = description;
            this.stackTrace = stackTrace;
            this.fullStackTrace = fullStackTrace;
            this.exceptionWrapper = exceptionWrapper;
        }

        private String exceptionString(String className,
                                       String description,
                                       List<StackTraceElement> stackTrace) {
            String desc = description == null ? "" : description;
            StringBuilder sb = new StringBuilder("");
            if (stackTrace != null) {
                for (StackTraceElement element : stackTrace) {
                    sb.append("        ").append(element).append("\n");
                }
            }
            return format("%s: %s\n%s", className, desc, sb.toString());
        }

        @Override
        public String toErrorString() {
            if (fullStackTrace == null) {
                // fullStackTrace is added in 1.2.0
                // If fullStackTrace is null, use the old error string for backward compatibility
                exceptionString(className, description, stackTrace);
            }
            return fullStackTrace;
        }
    }

    class TaskResultLost extends TaskFailedReason {
        @Override
        public String toErrorString() {
            return "TaskResultLost (result lost from block manager)";
        }
    }

    class TaskKilled extends TaskFailedReason {

        private String reason;

        public TaskKilled(String reason) {
            this.reason = reason;
        }

        @Override
        public String toErrorString() {
            return format("TaskKilled (%s)", reason);
        }

        @Override
        public boolean countTowardsTaskFailures() {
            return false;
        }
    }

    class TaskCommitDenied extends TaskFailedReason {

        private int jobID;
        private int partitionID;
        private int attemptNumber;

        public TaskCommitDenied(int jobID, int partitionID, int attemptNumber) {
            this.jobID = jobID;
            this.partitionID = partitionID;
            this.attemptNumber = attemptNumber;
        }

        @Override
        public String toErrorString() {
            return format("TaskCommitDenied (Driver denied task commit) " +
                    "for job: %d, partition: %d, attemptNumber: %d", jobID, partitionID, attemptNumber);
        }

        @Override
        public boolean countTowardsTaskFailures() {
            return false;
        }
    }

    class ExecutorLostFailure extends TaskFailedReason {

        private String execId;
        private boolean exitCausedByApp = true;
        private String reason;

        public ExecutorLostFailure(String execId, boolean exitCausedByApp, String reason) {
            this.execId = execId;
            this.exitCausedByApp = exitCausedByApp;
            this.reason = reason;
        }

        public ExecutorLostFailure(String execId, String reason) {
            this.execId = execId;
            this.reason = reason;
        }

        @Override
        public String toErrorString() {
            String exitBehavior = exitCausedByApp ? "caused by one of the running tasks" : "unrelated to the running tasks";

            return format("ExecutorLostFailure (executor %s exited %s Reason: %s)", execId, exitBehavior,
                    reason != null ? reason : "");
        }
    }

    class UnknownReason extends TaskFailedReason {
        @Override
        public String toErrorString() {
            return "UnknownReason";
        }
    }

    class Resubmitted extends TaskFailedReason {
        @Override
        public String toErrorString() {
            return "Resubmitted (resubmitted due to lost executor)";
        }
    }

    class FetchFailed extends TaskFailedReason {

        private BlockManagerId bmAddress;
        private int shuffleId;
        private int mapId;
        private int reduceId;
        private String message;

        public FetchFailed(BlockManagerId bmAddress,
                           int shuffleId,
                           int mapId,
                           int reduceId,
                           String message) {
            this.bmAddress = bmAddress;
            this.shuffleId = shuffleId;
            this.mapId = mapId;
            this.reduceId = reduceId;
            this.message = message;
        }

        @Override
        public String toErrorString() {
            String bmAddressString = bmAddress == null ? "null" : bmAddress.toString();
            return format("FetchFailed(%s, shuffleId=%d, mapId=%d, reduceId=%d, message=\n%s\n)",
                                bmAddressString, shuffleId, mapId, reduceId, message);
        }
    }


    class ThrowableSerializationWrapper implements Serializable {

        private Throwable exception;

        private void writeObject(ObjectOutputStream out) throws IOException{
            out.writeObject(exception);
        }
        private void readObject(ObjectInputStream in) throws IOException {
            try {
                exception = (Throwable) in.readObject();
            } catch (Exception e) {
                throw new IOException("Task exception could not be deserialized", e);
            }
        }

    }
}
