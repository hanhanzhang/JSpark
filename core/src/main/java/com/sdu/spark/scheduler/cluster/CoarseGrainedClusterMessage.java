package com.sdu.spark.scheduler.cluster;

import lombok.AllArgsConstructor;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * @author hanhan.zhang
 * */
public interface CoarseGrainedClusterMessage extends Serializable {

    class RegisteredExecutor implements CoarseGrainedClusterMessage, RegisterExecutorResponse {}

    @AllArgsConstructor
    class RegisterExecutorFailed implements CoarseGrainedClusterMessage, RegisterExecutorResponse {
        public String message;
    }

    @AllArgsConstructor
    class RemoveExecutor implements CoarseGrainedClusterMessage {
        public String executorId;
        public String reason;
    }

    @AllArgsConstructor
    class LaunchTask implements CoarseGrainedClusterMessage {
        public ByteBuffer taskData;
    }

    @AllArgsConstructor
    class KillTask implements CoarseGrainedClusterMessage {
        public long taskId;
        public String executorId;
        public boolean interruptThread;
        public String reason;
    }

    class StopExecutor implements CoarseGrainedClusterMessage {}

    class Shutdown implements CoarseGrainedClusterMessage {}
}
