package com.sdu.spark.scheduler;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sdu.spark.SparkException;
import com.sdu.spark.rpc.RpcEndPointRef;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.scheduler.OutputCommitCoordinationMessage.*;
import com.sdu.spark.scheduler.TaskEndReason.*;
import com.sdu.spark.utils.RpcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link OutputCommitCoordinator}
 *
 * @author hanhan.zhang 
 * */
public class OutputCommitCoordinator {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutputCommitCoordinator.class);
    private static final int NO_AUTHORIZED_COMMITTER = -1;

    private class StageState implements Serializable {
        int[] authorizedCommitters;
        // key = partitionId, value = taskId
        Map<Integer, Set<Integer>> failures;

        public StageState(int numPartitions) {
            authorizedCommitters = new int[numPartitions];
            for (int i = 0; i < numPartitions; ++i) {
                authorizedCommitters[i] = NO_AUTHORIZED_COMMITTER;
            }
            failures = Maps.newHashMap();
        }
    }

    private SparkConf conf;
    private boolean isDriver;

    public RpcEndPointRef coordinatorRef;

    private Map<Integer, StageState> stageStates;

    public OutputCommitCoordinator(SparkConf conf, boolean isDriver) {
        this.conf = conf;
        this.isDriver = isDriver;

        this.stageStates = Maps.newHashMap();
    }

    public boolean isEmpty() {
        return stageStates.isEmpty();
    }

    public boolean canCommit(int stageId, int partitionId, int attemptNumber) {
        AskPermissionToCommitOutput msg = new AskPermissionToCommitOutput(stageId, partitionId, attemptNumber);
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
            Set<Integer> failureTasks = stageState.failures.get(partition);
            if (failureTasks == null) {
                failureTasks = Sets.newHashSet();
                stageState.failures.put(partition, failureTasks);
            }
            failureTasks.add(attemptNumber);
            if (stageState.authorizedCommitters[partition] == attemptNumber) {
                LOGGER.debug("Authorized committer (attemptNumber={}, stage={}, partition={}) failed; " +
                             "clearing lock", attemptNumber, stage, partition);
                stageState.authorizedCommitters[partition] = NO_AUTHORIZED_COMMITTER;
            }
        }
    }

    private synchronized boolean attemptFailed(StageState stageState,
                                               int partition,
                                               int attemptNumber) {
        Set<Integer> failureTasks = stageState.failures.get(partition);
        return failureTasks != null && failureTasks.contains(attemptNumber);
    }

    public boolean handleAskPermissionToCommit(int stageId, int partition, int attemptNumber) {
        StageState stageState = stageStates.get(stageId);
        if (stageState == null) {
            LOGGER.debug("Stage {} has completed, so not allowing attempt number {} of partition {} to commit",
                         stageId, attemptNumber, partition);
            return false;
        }

        if (attemptFailed(stageState, partition, attemptNumber)) {
            LOGGER.info("Denying attemptNumber={} to commit for stage={}, partition={} as task attempt {} has already failed.",
                        attemptNumber, stageId, partition, attemptNumber);
            return false;
        }
        int existingCommitter = stageState.authorizedCommitters[partition];
        if (existingCommitter == NO_AUTHORIZED_COMMITTER) {
            LOGGER.debug("Authorizing attemptNumber={} to commit for stage={}, partition={}",
                         attemptNumber, stageId, partition);
            stageState.authorizedCommitters[partition] = attemptNumber;
            return true;
        }
        if (existingCommitter == attemptNumber) {
            LOGGER.warn("Authorizing duplicate request to commit for attemptNumber={} to commit for stage={}, " +
                        "partition={}; existingCommitter = {}. This can indicate dropped network traffic.",
                        attemptNumber, stageId, partition, existingCommitter);
            return true;
        }

        LOGGER.debug("Denying attemptNumber={} to commit for stage={}, partition={}; existingCommitter={}",
                      attemptNumber, stageId, partition, existingCommitter);
        return false;
    }

    public void stop() {
        if (isDriver) {
            coordinatorRef.send(new StopCoordinator());
            coordinatorRef = null;
            stageStates.clear();
        }
    }
}
