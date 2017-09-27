package com.sdu.spark;

import com.google.common.collect.Lists;
import com.sdu.spark.broadcast.Broadcast;
import com.sdu.spark.broadcast.BroadcastManager;
import com.sdu.spark.scheduler.MapStatus;
import com.sdu.spark.storage.BlockManagerId;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.util.List;

/**
 * {@link MapOutputTrackerMaster}辅助类, 记录ShuffleMapStage状态(一对一关系)
 *
 * @author hanhan.zhang 
 * */
public class ShuffleStatus {

    public int numPartitions;

    // ShuffleMapStage分区运行状态[index = partition_id]
    private MapStatus[] mapStatuses;

    // ShuffleMapStage分区运行状态序列化信息
    private byte[] cachedSerializedMapStatus;

    private Broadcast<byte[]> cachedSerializedBroadcast;

    private int numAvailableOutputs;

    public ShuffleStatus(int numPartitions) {
        this.numPartitions = numPartitions;

        this.mapStatuses = new MapStatus[this.numPartitions];
        this.numAvailableOutputs = 0;
    }

    public synchronized void addMapOutput(int mapId, MapStatus status) {
        if (mapStatuses[mapId] == null) {
            numAvailableOutputs++;
            invalidateSerializedMapOutputStatusCache();
        }
        mapStatuses[mapId] = status;
    }

    public synchronized void removeMapOutput(int mapId, BlockManagerId blockManagerId) {
        if (mapStatuses[mapId] != null && mapStatuses[mapId].location().equals(blockManagerId)) {
            mapStatuses[mapId] = null;
            numAvailableOutputs--;
            invalidateSerializedMapOutputStatusCache();
        }
    }

    public synchronized void removeOutputsOnHost(String host) {
        removeOutputsByFilter(x -> x.host.equals(host));
    }

    public synchronized void removeOutputsOnExecutor(String execId) {
        removeOutputsByFilter(x -> x.executorId.equals(execId));
    }

    private synchronized void removeOutputsByFilter(RemoveFilter removeFilter) {
        for (int i = 0; i < mapStatuses.length; ++i) {
            if (mapStatuses[i] != null && removeFilter.filter(mapStatuses[i].location())) {
                mapStatuses[i] = null;
                --numAvailableOutputs;
                invalidateSerializedMapOutputStatusCache();
            }
        }
    }

    public synchronized int numAvailableOutputs() {
        return numAvailableOutputs;
    }

    /**计算缺失的Partition, 重新计算*/
    public synchronized Integer[] findMissingPartitions() {
        List<Integer> missPartitions = Lists.newLinkedList();
        for (int i = 0; i < mapStatuses.length; ++i) {
            if (mapStatuses[i] == null) {
                missPartitions.add(i);
            }
        }
        assert missPartitions.size() == numPartitions - numAvailableOutputs :
                String.format("%d missing, expected %d", missPartitions.size(), numPartitions - numAvailableOutputs);

        return missPartitions.toArray(new Integer[missPartitions.size()]);
    }

    /**
     * {@link #mapStatuses}序列化, 发送到Reduce端
     * */
    public synchronized byte[] serializedMapStatus(BroadcastManager broadcastManager,
                                                   boolean isLocal, int minBroadcastSize) {
        try {
            if (cachedSerializedMapStatus == null) {
                Pair<byte[], Broadcast<byte[]>> serResult = MapOutputTracker.serializeMapStatuses(mapStatuses, broadcastManager, isLocal, minBroadcastSize);
                cachedSerializedMapStatus = serResult.getLeft();
                cachedSerializedBroadcast = serResult.getRight();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return cachedSerializedMapStatus;
    }

    // Used in testing.
    public synchronized boolean hasCachedSerializedBroadcast() {
        return cachedSerializedBroadcast != null;
    }


    public synchronized void invalidateSerializedMapOutputStatusCache() {
        if (cachedSerializedBroadcast != null) {
            // Prevent errors during broadcast cleanup from crashing the DAGScheduler (see SPARK-21444)
            cachedSerializedBroadcast.destroy(false);
            cachedSerializedBroadcast = null;
        }
        cachedSerializedMapStatus = null;
    }

    public synchronized <T> T withMapStatuses(ShuffleStatusMap<T> shuffleStatusMap) {
        return shuffleStatusMap.map(mapStatuses);
    }

    private interface RemoveFilter {
        boolean filter(BlockManagerId blockManagerId);
    }

    public interface ShuffleStatusMap<T> {
        T map(MapStatus[] mapStatuses);
    }
}
