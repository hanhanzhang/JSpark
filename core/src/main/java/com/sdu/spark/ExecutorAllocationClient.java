package com.sdu.spark;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * Cluster Manager客户端连接
 *
 * @author hanhan.zhang
 * */
public interface ExecutorAllocationClient {

    List<String> getExecutorIds();

    /**
     * @param numExecutors The total number of executors we'd like to have. The cluster manager
     *                     shouldn't kill any running executor to reach this number, but,
     *                     if all existing executors were to die, this is the number of executors
     *                     we'd want to be allocated.
     * @param localityAwareTasks The number of tasks in all active stages that have a locality
     *                           preferences. This includes running, pending, and completed tasks.
     * @param hostToLocalTaskCount A map of hosts to the number of tasks from all active stages
     *                             that would like to like to run on that host.
     *                             This includes running, pending, and completed tasks.
     * @return whether the request is acknowledged by the cluster manager.
     * */
    boolean requestTotalExecutors(int numExecutors,
                                  int localityAwareTasks,
                                  Map<String, Integer> hostToLocalTaskCount);

    boolean requestExecutors(int numAdditionalExecutors);

    List<String> killExecutors(List<String> executorIds, boolean replace, boolean force);

    default List<String> killExecutors(List<String> executorIds) {
        return killExecutors(executorIds, false, false);
    }

    boolean killExecutorsOnHost(String host);

    default boolean killExecutor(String executorId) {
        List<String> killedExecutors = killExecutors(Lists.newArrayList(executorId));
        return CollectionUtils.isNotEmpty(killedExecutors) && killedExecutors.get(0).equals(executorId);
    }

}
