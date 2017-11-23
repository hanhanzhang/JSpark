package com.sdu.spark.scheduler;

import com.sdu.spark.storage.BlockManagerId;

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
            return String.format("TaskKilled (%s)", reason);
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
            return String.format("TaskCommitDenied (Driver denied task commit) " +
                    "for job: %d, partition: %d, attemptNumber: %d", jobID, partitionID, attemptNumber);
        }

        @Override
        public boolean countTowardsTaskFailures() {
            return false;
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
            return String.format("FetchFailed(%s, shuffleId=%d, mapId=%d, reduceId=%d, message=\n%s\n)",
                                bmAddressString, shuffleId, mapId, reduceId, message);
        }
    }
}
