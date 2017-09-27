package com.sdu.spark;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sdu.spark.broadcast.BroadcastManager;
import com.sdu.spark.rpc.RpcCallContext;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.scheduler.MapStatus;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.BlockManagerId;
import com.sdu.spark.utils.ThreadUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import static com.sdu.spark.utils.RpcUtils.maxMessageSizeBytes;

/**
 * {@link MapOutputTrackerMaster}在Driver端启动:
 *
 *  1: {@link #shuffleStatuses} 维护ShuffleMapStage状态
 *
 *  2: {@link #mapOutputRequests}
 *
 * @author hanhan.zhang
 * */
public class MapOutputTrackerMaster extends MapOutputTracker {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapOutputTrackerMaster.class);

    private BroadcastManager broadcastManager;
    private boolean isLocal;

    private long minSizeForBroadcast;
    private boolean shuffleLocalityEnabled;

    // Number of map and reduce tasks above which we do not assign preferred locations based on map
    // output sizes. We limit the size of jobs for which assign preferred locations as computing the
    // top locations by size becomes expensive.
    private int SHUFFLE_PREF_MAP_THRESHOLD = 1000;
    // NOTE: This should be less than 2000 as we use HighlyCompressedMapStatus beyond that
    private int SHUFFLE_PREF_REDUCE_THRESHOLD = 1000;
    // Fraction of total map output that must be at a location for it to considered as a preferred
    // location for a reduce task. Making this larger will focus on fewer locations where most data
    // can be read locally, but may lead to more delay in scheduling if those locations are busy.
    private double REDUCER_PREF_LOCS_FRACTION = 0.2;

    private ConcurrentMap<Integer, ShuffleStatus> shuffleStatuses;

    private long maxRpcMessageSize;

    private LinkedBlockingQueue<GetMapOutputMessage> mapOutputRequests;
    // 标识退出消息处理线程
    private static final GetMapOutputMessage PoisonPill = new GetMapOutputMessage(-99, null);
    private ThreadPoolExecutor threadpool;

    public MapOutputTrackerMaster(SparkConf conf, BroadcastManager broadcastManager, boolean isLocal) {
        super(conf);
        this.broadcastManager = broadcastManager;
        this.isLocal = isLocal;

        this.shuffleLocalityEnabled = conf.getBoolean("spark.shuffle.reduceLocality.enabled", true);
        this.shuffleStatuses = Maps.newConcurrentMap();

        this.minSizeForBroadcast = conf.getSizeAsBytes("spark.shuffle.mapOutput.minSizeForBroadcast", "512K");
        this.maxRpcMessageSize = maxMessageSizeBytes(conf);
        if (minSizeForBroadcast > maxRpcMessageSize) {
            String msg = String.format("spark.shuffle.mapOutput.minSizeForBroadcast (%s bytes) must " +
                    "be <= spark.rpc.message.maxSize (%s bytes) to prevent sending an rpc " +
                    "message that is too large.", minSizeForBroadcast, maxRpcMessageSize);
            LOGGER.error(msg);
            throw new IllegalArgumentException(msg);
        }

        this.mapOutputRequests = new LinkedBlockingQueue<>();
        int numThreads = conf.getInt("spark.shuffle.mapOutput.dispatcher.numThreads", 8);
        this.threadpool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "map-output-dispatcher");
        for (int i = 0; i < numThreads; ++i) {
            this.threadpool.execute(new MessageLoop());
        }
    }

    public void post(GetMapOutputMessage msg) {
        mapOutputRequests.offer(msg);
    }

    public void registerShuffle(int shuffleId, int numMaps) {
        if (shuffleStatuses.putIfAbsent(shuffleId, new ShuffleStatus(numMaps)) == null) {
            throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice");
        }
    }

    public void registerMapOutput(int shuffleId, int mapId, MapStatus status) {
        shuffleStatuses.get(shuffleId).addMapOutput(mapId, status);
    }

    public int getNumCachedSerializedBroadcast() {
        int sum = 0;
        for (ShuffleStatus status : shuffleStatuses.values()) {
            if (status.hasCachedSerializedBroadcast()) {
                sum++;
            }
        }
        return sum;
    }

    @Override
    public Map<BlockManagerId, Pair<BlockId, Long>> getMapSizesByExecutorId(int shuffleId, int startPartition, int endPartition) {
        LOGGER.info("Fetching outputs for shuffle $shuffleId, partitions {}-{}", startPartition, endPartition);
        ShuffleStatus status = shuffleStatuses.get(shuffleId);
        if (status == null) {
            return Collections.emptyMap();
        }
        return status.withMapStatuses(mapStatuses ->
                MapOutputTracker.convertMapStatuses(shuffleId, startPartition, endPartition, mapStatuses));
    }

    public void unregisterMapOutput(int shuffleId, int mapId, BlockManagerId bmAddress) throws SparkException {
        ShuffleStatus status = shuffleStatuses.get(shuffleId);
        if (status == null) {
            throw new SparkException("unregisterMapOutput called for nonexistent shuffle ID");
        }
        status.removeMapOutput(mapId, bmAddress);
        incrementEpoch();
    }

    @Override
    public void unregisterShuffle(int shuffleId) {
        ShuffleStatus status = shuffleStatuses.remove(shuffleId);
        if (status != null) {
            status.invalidateSerializedMapOutputStatusCache();
        }
    }

    public void removeOutputsOnHost(String host) {
        shuffleStatuses.values().forEach(status -> status.removeOutputsOnHost(host));
        incrementEpoch();
    }

    public void removeOutputsOnExecutor(String execId) {
        shuffleStatuses.values().forEach(status -> status.removeOutputsOnExecutor(execId));
        incrementEpoch();
    }

    public boolean containsShuffle(int shuffleId) {
        return shuffleStatuses.containsKey(shuffleId);
    }

    public int getNumAvailableOutputs(int shuffleId) {
        ShuffleStatus status = shuffleStatuses.get(shuffleId);
        if (status == null) {
            return 0;
        }
        return status.numAvailableOutputs();
    }

    public Integer[] findMissingPartitions(int shuffleId) {
        ShuffleStatus status = shuffleStatuses.get(shuffleId);
        if (status == null) {
            throw new IllegalArgumentException("shuffleId " + shuffleId + " not exist");
        }
        return status.findMissingPartitions();
    }

    public MapOutputStatistics getStatistics(ShuffleDependency<?, ?, ?> dep) {
        ShuffleStatus status = shuffleStatuses.get(dep.shuffleId);
        if (status == null) {
            throw new RuntimeException("shuffleId " + dep.shuffleId + " not exist");
        }

        return status.withMapStatuses(mapStatuses -> {
            long[] totalSizes = new long[dep.partitioner.numPartitions()];
            for (int j = 0; j < mapStatuses.length; ++j) {
                for (int i = 0; i < totalSizes.length; ++i) {
                    totalSizes[i] += mapStatuses[i].getSizeForBlock(j);
                }
            }
            return new MapOutputStatistics(dep.shuffleId, totalSizes);
        });
    }

    /**
     * 计算Shuffle依赖的Block的地址
     * */
    public String[] getPreferredLocationsForShuffle(ShuffleDependency<?, ?, ?> dep, int partitionId) {
        if (shuffleLocalityEnabled && dep.rdd().partitions().size() < SHUFFLE_PREF_MAP_THRESHOLD &&
                dep.partitioner.numPartitions() < SHUFFLE_PREF_REDUCE_THRESHOLD) {
            BlockManagerId[] blockManagerIds = getLocationsWithLargestOutputs(dep.shuffleId, partitionId,
                    dep.partitioner.numPartitions(), REDUCER_PREF_LOCS_FRACTION);
            if (blockManagerIds.length != 0) {
                String[] host = new String[blockManagerIds.length];
                for (int i = 0; i < blockManagerIds.length; ++i) {
                    host[i] = blockManagerIds[i].host;
                }
                return host;
            }
        }
        return new String[0];
    }


    private BlockManagerId[] getLocationsWithLargestOutputs(int shuffleId, int reducerId,
                                                            int numReducers, double fractionThreshold) {
        ShuffleStatus shuffleStatus = shuffleStatuses.get(shuffleId);
        if (shuffleStatus != null) {
            return shuffleStatus.withMapStatuses(mapStatuses -> {
                List<BlockManagerId> blockManagerIds = Lists.newArrayList();
                if (mapStatuses.length != 0) {
                    Map<BlockManagerId, Long> locs = Maps.newHashMap();
                    int totalSize = 0;
                    for (int i = 0; i < mapStatuses.length; ++i) {
                        MapStatus status = mapStatuses[i];
                        // status在初始化或移除时, 置为null
                        if (status != null) {
                            long blockSize = status.getSizeForBlock(reducerId);
                            if (blockSize > 0) {
                                long oldBlockSize = locs.getOrDefault(status.location(), 0L);
                                locs.put(status.location(), oldBlockSize + blockSize);
                                totalSize += blockSize;
                            }
                        }
                    }

                    for (Map.Entry<BlockManagerId, Long> entry : locs.entrySet()) {
                        BlockManagerId blockManagerId = entry.getKey();
                        long blockSize = entry.getValue();
                        if (((double) blockSize) / (double) totalSize >= fractionThreshold) {
                            blockManagerIds.add(blockManagerId);
                        }
                    }
                }
                return blockManagerIds.toArray(new BlockManagerId[blockManagerIds.size()]);
            });


        }
        return new BlockManagerId[0];
    }

    @Override
    public void stop() {

    }

    public void incrementEpoch() {
        synchronized (epochLock) {
            epoch += 1;
            LOGGER.debug("Increasing epoch to {}", epoch);
        }
    }

    public long getEpoch() {
        synchronized (epochLock) {
            return epoch;
        }
    }

    private class MessageLoop implements Runnable {
        @Override
        public void run() {
            try {
                GetMapOutputMessage message = mapOutputRequests.take();
                if (message.equals(PoisonPill)) {
                    // Put PoisonPill back so that other MessageLoops can see it.
                    mapOutputRequests.offer(PoisonPill);
                    return;
                }

                RpcCallContext context = message.context;
                int shuffleId = message.shuffleId;
                String hostPort = context.senderAddress().hostPort();
                LOGGER.debug("Handling request to send map output locations for shuffle {} to {} ", shuffleId, hostPort);
                ShuffleStatus shuffleStatus = shuffleStatuses.get(shuffleId);
                context.reply(
                        shuffleStatus.serializedMapStatus(broadcastManager, isLocal, (int) minSizeForBroadcast));
            } catch (InterruptedException e) {
                // exit
            }
        }
    }
}
