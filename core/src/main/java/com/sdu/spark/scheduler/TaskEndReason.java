package com.sdu.spark.scheduler;

import java.io.Serializable;

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
            return String.format("TaskCommitDenied (Driver denied task commit) " +
                    "for job: %d, partition: %d, attemptNumber: %d", jobID, partitionID, attemptNumber);
        }

        @Override
        public boolean countTowardsTaskFailures() {
            return false;
        }
    }
}
