package com.sdu.spark.scheduler.cluster;

import com.sdu.spark.rpc.RpcEndPointRef;
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
        public ByteBuffer taskData;
    }

    @AllArgsConstructor
    class KillTask implements CoarseGrainedClusterMessage {
        public long taskId;
        public String executorId;
        public boolean interruptThread;
        public String reason;
    }

    class RetrieveSparkAppConfig implements CoarseGrainedClusterMessage {}

    @AllArgsConstructor
    class SparkAppConfig implements CoarseGrainedClusterMessage {
        public Properties sparkProperties;
        public byte[] ioEncryptionKey;
    }

    class StopExecutor implements CoarseGrainedClusterMessage {}

    class Shutdown implements CoarseGrainedClusterMessage {}
}
