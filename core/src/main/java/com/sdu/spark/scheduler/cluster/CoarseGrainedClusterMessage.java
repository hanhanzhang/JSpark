package com.sdu.spark.scheduler.cluster;

import com.google.common.base.MoreObjects;
import com.sdu.spark.executor.ExecutorExitCode.*;
import com.sdu.spark.rpc.RpcEndpointRef;
import com.sdu.spark.scheduler.TaskState;
import com.sdu.spark.utils.SerializableBuffer;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;

/**
 * @author hanhan.zhang
 * */
public interface CoarseGrainedClusterMessage extends Serializable {

    class RegisteredExecutor implements CoarseGrainedClusterMessage, RegisterExecutorResponse {}

    class RegisterExecutorFailed implements CoarseGrainedClusterMessage, RegisterExecutorResponse {
        public String message;

        public RegisterExecutorFailed(String message) {
            this.message = message;
        }
    }

    class LaunchTask implements CoarseGrainedClusterMessage {
        public SerializableBuffer taskData;

        public LaunchTask(SerializableBuffer taskData) {
            this.taskData = taskData;
        }
    }
    class RetrieveSparkAppConfig implements CoarseGrainedClusterMessage {}

    class SparkAppConfig implements CoarseGrainedClusterMessage {
        public Properties sparkProperties;
        public byte[] ioEncryptionKey;

        public SparkAppConfig(Properties sparkProperties, byte[] ioEncryptionKey) {
            this.sparkProperties = sparkProperties;
            this.ioEncryptionKey = ioEncryptionKey;
        }
    }

    class StopExecutor implements CoarseGrainedClusterMessage {}


    class Shutdown implements CoarseGrainedClusterMessage {}

    class StopDriver implements CoarseGrainedClusterMessage {}


    /*********************Spark Driver Message(CoarseGrainedSchedulerBackend.DriverEndpoint)*****************/
    class ReviveOffers implements CoarseGrainedClusterMessage {}

    class StatusUpdate implements CoarseGrainedClusterMessage {
        public String executorId;
        public long taskId;
        public TaskState state;
        public SerializableBuffer data;

        public StatusUpdate(String executorId,
                            long taskId,
                            TaskState state,
                            ByteBuffer data) {
            this.executorId = executorId;
            this.taskId = taskId;
            this.state = state;
            this.data = new SerializableBuffer(data);
        }

        @Override
        public String toString() {
           return MoreObjects.toStringHelper(this)
                             .add("execId", executorId)
                             .add("taskId", taskId)
                             .add("state", state.name())
                             .toString();
        }
    }

    class RegisterExecutor implements CoarseGrainedClusterMessage {
        public String executorId;
        public RpcEndpointRef executorRef;
        public String hostname;
        public int cores;
        public Map<String, String> logUrls;

        public RegisterExecutor(String executorId, RpcEndpointRef executorRef, String hostname,
                                int cores, Map<String, String> logUrls) {
            this.executorId = executorId;
            this.executorRef = executorRef;
            this.hostname = hostname;
            this.cores = cores;
            this.logUrls = logUrls;
        }
    }

    class RemoveExecutor implements CoarseGrainedClusterMessage {
        public String executorId;
        public ExecutorLossReason reason;

        public RemoveExecutor(String executorId, ExecutorLossReason reason) {
            this.executorId = executorId;
            this.reason = reason;
        }
    }

    class KillExecutorsOnHost implements CoarseGrainedClusterMessage {
        public String host;

        public KillExecutorsOnHost(String host) {
            this.host = host;
        }
    }

    class StopExecutors implements CoarseGrainedClusterMessage {}

    class KillTask implements CoarseGrainedClusterMessage {
        public long taskId;
        public String executorId;
        public boolean interruptThread;
        public String reason;

        public KillTask(long taskId, String executorId, boolean interruptThread, String reason) {
            this.taskId = taskId;
            this.executorId = executorId;
            this.interruptThread = interruptThread;
            this.reason = reason;
        }
    }

    class RemoveWorker implements CoarseGrainedClusterMessage {
        public String workerId;
        public String host;
        public String message;

        public RemoveWorker(String workerId, String host, String message) {
            this.workerId = workerId;
            this.host = host;
            this.message = message;
        }
    }

}
