package com.sdu.spark.scheduler.cluster;

import com.sdu.spark.rpc.RpcEndPointRef;
import com.sdu.spark.scheduler.TaskState;
import com.sdu.spark.utils.SerializableBuffer;
import lombok.AllArgsConstructor;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;

/**
 * @author hanhan.zhang
 * */
public interface CoarseGrainedClusterMessage extends Serializable {

    @AllArgsConstructor
    class RegisterExecutor implements CoarseGrainedClusterMessage {
        public String executorId;
        public RpcEndPointRef executorRef;
        public String hostname;
        public int cores;
        public Map<String, String> logUrls;
    }

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
        public SerializableBuffer taskData;
    }

    @AllArgsConstructor
    class KillTask implements CoarseGrainedClusterMessage {
        public long taskId;
        public String executorId;
        public boolean interruptThread;
        public String reason;
    }

    @AllArgsConstructor
    class KillExecutorsOnHost implements CoarseGrainedClusterMessage {
        public String host;
    }

    class RetrieveSparkAppConfig implements CoarseGrainedClusterMessage {}

    @AllArgsConstructor
    class SparkAppConfig implements CoarseGrainedClusterMessage {
        public Properties sparkProperties;
        public byte[] ioEncryptionKey;
    }

    class StopExecutor implements CoarseGrainedClusterMessage {}

    class StopExecutors implements CoarseGrainedClusterMessage {}

    class Shutdown implements CoarseGrainedClusterMessage {}

    class ReviveOffers implements CoarseGrainedClusterMessage {}

    class StopDriver implements CoarseGrainedClusterMessage {}

    @AllArgsConstructor
    class RemoveWorker implements CoarseGrainedClusterMessage {
        public String workerId;
        public String host;
        public String message;
    }

    class StatusUpdate implements CoarseGrainedClusterMessage {
        public String executorId;
        public long taskId;
        public TaskState state;
        public SerializableBuffer data;

        public StatusUpdate(String executorId, long taskId, TaskState state, ByteBuffer data) {
            this.executorId = executorId;
            this.taskId = taskId;
            this.state = state;
            this.data = new SerializableBuffer(data);
        }
    }
}
