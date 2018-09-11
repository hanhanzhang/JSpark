package com.sdu.spark.scheduler;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public interface OutputCommitCoordinationMessage extends Serializable {

    class StopCoordinator implements OutputCommitCoordinationMessage {}

    class AskPermissionToCommitOutput implements OutputCommitCoordinationMessage {
        public int stageId;
        public int stageAttempt;
        public int partition;
        public int attemptNumber;

        public AskPermissionToCommitOutput(int stageId, int stageAttempt, int partition, int attemptNumber) {
            this.stageId = stageId;
            this.stageAttempt = stageAttempt;
            this.partition = partition;
            this.attemptNumber = attemptNumber;
        }
    }

}
