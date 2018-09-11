package com.sdu.spark.scheduler;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sdu.spark.SparkException;
import com.sdu.spark.rpc.RpcEndpointRef;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.scheduler.OutputCommitCoordinationMessage.AskPermissionToCommitOutput;
import com.sdu.spark.scheduler.OutputCommitCoordinationMessage.StopCoordinator;
import com.sdu.spark.scheduler.TaskEndReason.Success;
import com.sdu.spark.scheduler.TaskEndReason.TaskCommitDenied;
import com.sdu.spark.utils.RpcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * OutputCommitCoordinator授权是否将Task将输出落地HDFS
 *
 * Driver及Executor都会实例化OutputCommitCoordinator, 在Driver上注册OutputCommitCoordinatorEndpoint, 在Executor上
 * 通过OutputCommitCoordinatorEndpoint的RpcEndpointRef询问授权
 *
 * @author hanhan.zhang 
 * */
public class OutputCommitCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutputCommitCoordinator.class);

    private class StageState implements Serializable {
        TaskIdentifier[] authorizedCommitters;
        // key = partitionId, value = taskId
        Map<Integer, Set<Integer>> failures;

        StageState(int numPartitions) {
            authorizedCommitters = new TaskIdentifier[numPartitions];
            for (int i = 0; i < numPartitions; ++i) {
                authorizedCommitters[i] = null;
            }
            failures = Maps.newHashMap();
        }
    }

    private SparkConf conf;
    private boolean isDriver;

    public RpcEndpointRef coordinatorRef;

    private Map<Integer, StageState> stageStates;

    public OutputCommitCoordinator(SparkConf conf, boolean isDriver) {
        this.conf = conf;
        this.isDriver = isDriver;

        this.stageStates = Maps.newHashMap();
    }

    public boolean isEmpty() {
        return stageStates.isEmpty();
    }

    public boolean canCommit(int stageId,
                             int stageAttempt,
                             int partitionId,
                             int attemptNumber) {
        AskPermissionToCommitOutput msg = new AskPermissionToCommitOutput(stageId, stageAttempt, partitionId, attemptNumber);
        if (coordinatorRef == null) {
            LOGGER.error("canCommit called after coordinator was stopped (is SparkEnv shutdown in progress)?");
            return false;
        }
        Future<Boolean> res = coordinatorRef.ask(msg);
        try {
            return res.get(RpcUtils.getRpcAskTimeout(conf), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new SparkException("Exception thrown in awaitResult: " + e.getMessage());
        }
    }

    /**
     * * Called by the DAGScheduler when a stage starts.
     *
     * @param stage the stage id.
     * @param maxPartitionId the maximum partition id that could appear in this stage's tasks (i.e.
     *                       the maximum possible value of `context.partitionId`).
     * */
    public synchronized void stageStart(int stage, int maxPartitionId) {
        StageState state = new StageState(maxPartitionId + 1);
        stageStates.put(stage, state);
    }

    // Called by DAGScheduler
    public synchronized void stageEnd(int stage) {
        stageStates.remove(stage);
    }

    // Called by DAGScheduler
    public synchronized void taskCompleted(int stage,
                                           int stageAttempt,
                                           int partition,
                                           int attemptNumber,
                                           TaskEndReason reason) {
        StageState stageState = stageStates.get(stage);
        if (stageState == null) {
            LOGGER.debug("Ignoring task completion for completed stage");
            return;
        }
        if (reason instanceof Success) {
            // The task output has been committed successfully
        } else if (reason instanceof TaskCommitDenied) {
            LOGGER.info("Task was denied committing, stage: {}, partition: {}, attempt: {}",
                        stage, partition, attemptNumber);
        } else {
            TaskIdentifier taskId = new TaskIdentifier(stageAttempt, attemptNumber);
            Set<Integer> failureTasks = stageState.failures.computeIfAbsent(partition, (key) -> Sets.newHashSet());
            failureTasks.add(attemptNumber);
            if (stageState.authorizedCommitters[partition] != null &&
                    stageState.authorizedCommitters[partition].equals(taskId)) {
                LOGGER.debug("Authorized committer (attemptNumber={}, stage={}, partition={}) failed; " +
                             "clearing lock", attemptNumber, stage, partition);
                stageState.authorizedCommitters[partition] = null;
            }
        }
    }

    private synchronized boolean attemptFailed(StageState stageState,
                                               int partition,
                                               int attemptNumber) {
        Set<Integer> failureTasks = stageState.failures.get(partition);
        return failureTasks != null && failureTasks.contains(attemptNumber);
    }

    public boolean handleAskPermissionToCommit(int stageId,
                                               int stageAttempt,
                                               int partition,
                                               int attemptNumber) {
        StageState stageState = stageStates.get(stageId);
        if (stageState == null) {
            LOGGER.debug("Commit denied for stage={}.{}, partition={}: stage already marked as completed.",
                    stageId, stageAttempt, partition);
            return false;
        }

        if (attemptFailed(stageState, partition, attemptNumber)) {
            LOGGER.debug("Commit denied for stage={}.{}, partition={}: task attempt {} already marked as failed.",
                    stageId, stageAttempt, partition, attemptNumber);
            return false;
        }
        TaskIdentifier existingCommitter = stageState.authorizedCommitters[partition];
        if (existingCommitter == null) {
            LOGGER.debug("Commit allowed for stage={}.{}, partition={}, task attempt {}",
                    stageId, stageAttempt, partition, attemptNumber);
            stageState.authorizedCommitters[partition] = new TaskIdentifier(stageAttempt, attemptNumber);
            return true;
        }

        LOGGER.debug("Commit denied for stage={}.{}, partition={}: already committed by {}",
                stageId, stageAttempt, partition, existingCommitter);
        return false;
    }

    public void stop() {
        if (isDriver) {
            coordinatorRef.send(new StopCoordinator());
            coordinatorRef = null;
            stageStates.clear();
        }
    }

    private class TaskIdentifier implements Serializable {
        int stageAttempt;
        int taskAttempt;

        TaskIdentifier(int stageAttempt, int taskAttempt) {
            this.stageAttempt = stageAttempt;
            this.taskAttempt = taskAttempt;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) return true;
            if (object == null || getClass() != object.getClass()) return false;

            TaskIdentifier that = (TaskIdentifier) object;

            if (stageAttempt != that.stageAttempt) return false;
            return taskAttempt == that.taskAttempt;
        }

        @Override
        public int hashCode() {
            int result = stageAttempt;
            result = 31 * result + taskAttempt;
            return result;
        }

        @Override
        public String toString() {
            return "TaskIdentifier[" +
                    "stageAttempt=" + stageAttempt +
                    ", taskAttempt=" + taskAttempt +
                    ']';
        }
    }
}
