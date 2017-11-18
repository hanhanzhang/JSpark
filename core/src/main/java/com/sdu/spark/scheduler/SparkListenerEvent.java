package com.sdu.spark.scheduler;

import com.sdu.spark.executor.ExecutorExitCode.*;
import com.sdu.spark.scheduler.cluster.ExecutorInfo;
import com.sdu.spark.storage.BlockManagerId;
import com.sdu.spark.storage.BlockUpdatedInfo;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 *
 * @author hanhan.zhang
 * */
public interface SparkListenerEvent extends Serializable {

    default boolean logEvent() {
        return true;
    }

    class SparkListenerStageSubmitted implements SparkListenerEvent {
        public StageInfo stageInfo;
        public Properties properties;

        public SparkListenerStageSubmitted(StageInfo stageInfo) {
            this(stageInfo, null);
        }

        public SparkListenerStageSubmitted(StageInfo stageInfo, Properties properties) {
            this.stageInfo = stageInfo;
            this.properties = properties;
        }
    }

    @AllArgsConstructor
    class SparkListenerStageCompleted implements SparkListenerEvent {
        public StageInfo stageInfo;
    }

    @AllArgsConstructor
    class SparkListenerTaskStart implements SparkListenerEvent {
        public int stageId;
        public int stageAttemptId;
        public TaskInfo taskInfo;
    }

    @AllArgsConstructor
    class SparkListenerTaskGettingResult implements SparkListenerEvent {
        public TaskInfo taskInfo;
    }

    @AllArgsConstructor
    class SparkListenerTaskEnd implements SparkListenerEvent {
        public int stageId;
        public int stageAttemptId;
        public String taskType;
        public TaskEndReason reason;
        public TaskInfo taskInfo;
    }

    class SparkListenerJobStart implements SparkListenerEvent {
        public int jobId;
        public long time;
        public List<StageInfo> stageInfos;
        public Properties properties;
        public List<Integer> stageIds;

        public SparkListenerJobStart(int jobId, long time, List<StageInfo> stageInfos) {
            this(jobId, time, stageInfos, null);
        }

        public SparkListenerJobStart(int jobId, long time, List<StageInfo> stageInfos, Properties properties) {
            this.jobId = jobId;
            this.time = time;
            this.stageInfos = stageInfos;
            this.properties = properties;
            this.stageIds = this.stageInfos.stream().map(stageInfo -> stageInfo.stageId).collect(Collectors.toList());
        }
    }

    @AllArgsConstructor
    class SparkListenerJobEnd implements SparkListenerEvent {
        public int jobId;
        public long time;
        public JobResult jobResult;
    }

    @AllArgsConstructor
    class SparkListenerEnvironmentUpdate implements SparkListenerEvent {
        public Map<String, List<Pair<String, String>>> environmentDetails;
    }

    class SparkListenerBlockManagerAdded implements SparkListenerEvent {
        public long time;
        public BlockManagerId blockManagerId;
        public long maxMem;
        public long maxOnHeapMem;
        public long maxOffHeapMem;

        public SparkListenerBlockManagerAdded(long time, BlockManagerId blockManagerId, long maxMem) {
            this(time, blockManagerId, maxMem, 0L, 0L);
        }

        public SparkListenerBlockManagerAdded(long time, BlockManagerId blockManagerId, long maxMem,
                                              long maxOnHeapMem, long maxOffHeapMem) {
            this.time = time;
            this.blockManagerId = blockManagerId;
            this.maxMem = maxMem;
            this.maxOnHeapMem = maxOnHeapMem;
            this.maxOffHeapMem = maxOffHeapMem;
        }
    }


    @AllArgsConstructor
    class SparkListenerBlockManagerRemoved implements SparkListenerEvent {
        public long time;
        public BlockManagerId blockManagerId;
    }


    @AllArgsConstructor
    class SparkListenerUnpersistRDD implements SparkListenerEvent {
        public int rddId;
    }

    @AllArgsConstructor
    class SparkListenerExecutorAdded implements SparkListenerEvent {
        public long time;
        public String executorId;
        public ExecutorInfo executorInfo;
    }

    @AllArgsConstructor
    class SparkListenerExecutorRemoved implements SparkListenerEvent {
        public long time;
        public String executorId;
        public ExecutorLossReason reason;
    }

    @AllArgsConstructor
    class SparkListenerExecutorBlacklisted implements SparkListenerEvent {
        public long time;
        public String executorId;
        public int taskFailures;
    }

    @AllArgsConstructor
    class SparkListenerExecutorUnblacklisted implements SparkListenerEvent {
        public long time;
        public String executorId;
    }

    @AllArgsConstructor
    class SparkListenerNodeBlacklisted implements SparkListenerEvent {
        public long time;
        public String hostId;
        public int executorFailures;
    }

    @AllArgsConstructor
    class SparkListenerNodeUnblacklisted implements SparkListenerEvent {
        public long time;
        public String hostId;
    }

    @AllArgsConstructor
    class SparkListenerBlockUpdated implements SparkListenerEvent {
        public BlockUpdatedInfo blockUpdatedInfo;
    }

    @AllArgsConstructor
    class SparkListenerApplicationStart implements SparkListenerEvent {
        public String appName;
        public String appId;
        public long time;
        public String appAttemptId;
    }

    @AllArgsConstructor
    class SparkListenerApplicationEnd implements SparkListenerEvent {
        public long time;
    }

}
