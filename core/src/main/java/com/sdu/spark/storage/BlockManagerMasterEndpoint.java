package com.sdu.spark.storage;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sdu.spark.rpc.*;
import com.sdu.spark.scheduler.LiveListenerBus;
import com.sdu.spark.scheduler.SparkListenerEvent.SparkListenerBlockManagerAdded;
import com.sdu.spark.scheduler.SparkListenerEvent.SparkListenerBlockManagerRemoved;
import com.sdu.spark.storage.BlockManagerMessages.*;
import com.sdu.spark.utils.ThreadUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

import static com.sdu.spark.utils.Utils.bytesToString;

/**
 *
 * {@link BlockManagerMasterEndpoint} 维护BlockManager上Block数据存储信息
 *
 * todo: {@link TopologyMapper} 作用
 * todo: {@link #removeBlockManager(BlockManagerId)} 副本的删除
 *
 * @author hanhan.zhang
 * */
public class BlockManagerMasterEndpoint extends ThreadSafeRpcEndpoint {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlockManagerMasterEndpoint.class);

    private boolean isLocal;
    private SparkConf conf;
    private LiveListenerBus listenerBus;

    private Map<BlockManagerId, BlockManagerInfo> blockManagerInfo;
    private Map<String, BlockManagerId> blockManagerIdByExecutor;
    // Block数据可能会有副本
    private Map<BlockId, Set<BlockManagerId>> blockLocations;

    private ThreadPoolExecutor askThreadPool;
    private boolean proactivelyReplicate;

    private TopologyMapper topologyMapper;

    public BlockManagerMasterEndpoint(RpcEnv rpcEnv, boolean isLocal,
                                      SparkConf conf, LiveListenerBus listenerBus) {
        super(rpcEnv);
        this.isLocal = isLocal;
        this.conf = conf;
        this.listenerBus = listenerBus;

        this.blockManagerInfo = Maps.newHashMap();
        this.blockManagerIdByExecutor = Maps.newHashMap();
        this.blockLocations = Maps.newHashMap();

        this.askThreadPool = ThreadUtils.newDaemonCachedThreadPool("block-manager-ask-thread-pool");
        this.proactivelyReplicate = conf.getBoolean("spark.storage.replication.proactive", false);
    }

    @Override
    public void receiveAndReply(Object msg, RpcCallContext context) {
        if (msg instanceof RegisterBlockManager) {
            BlockManagerId id = register((RegisterBlockManager) msg);
            context.reply(id);
        } else if (msg instanceof UpdateBlockInfo) {
            boolean result = updateBlockInfo((UpdateBlockInfo) msg);
            context.reply(result);
        } else if (msg instanceof GetLocations) {
            Set<BlockManagerId> locations = getLocation(((GetLocations) msg).blockId);
            context.reply(locations);
        } else if (msg instanceof GetLocationsMultipleBlockIds) {
            Set<BlockManagerId>[] locations = getLocationsMultipleBlockIds(((GetLocationsMultipleBlockIds) msg).blockIds);
            context.reply(locations);
        } else if (msg instanceof GetPeers) {
            Set<BlockManagerId> peers = getPeers(((GetPeers) msg).blockManagerId);
            context.reply(peers);
        } else if (msg instanceof GetExecutorEndpointRef) {
            RpcEndPointRef ref = getExecutorEndpointRef(((GetExecutorEndpointRef) msg).execId);
            context.reply(ref);
        } else if (msg instanceof GetMemoryStatus) {
            context.reply(memoryStatus());
        } else if (msg instanceof GetStorageStatus) {
            // TODO:  
        } else if (msg instanceof GetBlockStatus) {
            // TODO:
        } else if (msg instanceof GetMatchingBlockIds) {

        } else if (msg instanceof RemoveRdd) {

        } else if (msg instanceof RemoveShuffle) {

        } else if (msg instanceof RemoveBroadcast) {

        } else if (msg instanceof RemoveBlock) {

        } else if (msg instanceof RemoveExecutor) {

        } else if (msg instanceof StopBlockManagerMaster) {

        } else if (msg instanceof BlockManagerHeartbeat) {

        } else if (msg instanceof HasCachedBlocks) {

        }
    }

    private BlockManagerId register(RegisterBlockManager manager) {
        BlockManagerId id = BlockManagerId.apply(manager.blockManagerId.executorId,
                                                             manager.blockManagerId.host,
                                                             manager.blockManagerId.port,
                                                             topologyMapper.getTopologyForHost(manager.blockManagerId.host));
        long time = System.currentTimeMillis();
        if (!blockManagerInfo.containsKey(id)) {
            BlockManagerId oldId = blockManagerIdByExecutor.get(id.executorId);
            if (oldId != null) {
                // A block manager of the same executor already exists, so remove it (assumed dead)
                LOGGER.error("Got two different block manager registrations on same executor - will replace old one {} with new one {}", oldId, id);
                removeExecutor(id.executorId);
            }

            LOGGER.info("Registering block manager {} with {} RAM, {}", id.hostPort(),
                    bytesToString(manager.maxOffHeapMemSize + manager.maxOnHeapMemSize), id);

            blockManagerIdByExecutor.put(id.executorId, id);
            blockManagerInfo.put(id, new BlockManagerInfo(id, manager.maxOnHeapMemSize,
                                                          manager.maxOffHeapMemSize, manager.sender,
                                                          System.currentTimeMillis()));
        }

        listenerBus.post(new SparkListenerBlockManagerAdded(time, id, manager.maxOnHeapMemSize + manager.maxOffHeapMemSize,
                                                            manager.maxOnHeapMemSize, manager.maxOffHeapMemSize));
        return id;
    }

    private boolean updateBlockInfo(UpdateBlockInfo updateBlockInfo) {
        if (!blockManagerInfo.containsKey(updateBlockInfo.blockManagerId)) {
            // We intentionally do not register the master (except in local mode),
            // so we should not indicate failure.
            if (updateBlockInfo.blockManagerId.isDriver() && !isLocal) {
                return true;
            }
            return false;
        }

        BlockManagerInfo managerInfo = blockManagerInfo.get(updateBlockInfo.blockManagerId);
        if (updateBlockInfo.blockId == null) {
            managerInfo.updateLastSeenMs();
            return true;
        }
        managerInfo.updateBlockInfo(updateBlockInfo.blockId, updateBlockInfo.storageLevel,
                                    updateBlockInfo.memSize, updateBlockInfo.diskSize);

        // 更新数据块存储地址信息
        Set<BlockManagerId> locations = blockLocations.get(updateBlockInfo.blockId);
        if (locations == null) {
            locations = Sets.newHashSet();
            blockLocations.put(updateBlockInfo.blockId, locations);
        }
        if (updateBlockInfo.storageLevel.isValid()) {
            locations.add(updateBlockInfo.blockManagerId);
        } else {
            locations.remove(updateBlockInfo.blockManagerId);
        }
        // Remove the block from master tracking if it has been removed on all slaves.
        if (locations.size() == 0) {
            blockLocations.remove(updateBlockInfo.blockId);
        }

        return true;
    }

    private Set<BlockManagerId> getLocation(BlockId blockId) {
        Set<BlockManagerId> locations = blockLocations.get(blockId);
        return locations == null ? Collections.emptySet() : locations;
    }

    private Set<BlockManagerId>[] getLocationsMultipleBlockIds(BlockId[] blockIds) {
        Set<BlockManagerId>[] locations = new Set[blockIds.length];
        for (int i = 0; i < blockIds.length; ++i) {
            locations[i] = getLocation(blockIds[i]);
        }
        return locations;
    }

    private Set<BlockManagerId> getPeers(BlockManagerId managerId) {
        return blockManagerInfo.keySet().stream().filter(blockManagerId -> !blockManagerId.isDriver())
                                          .filter(blockManagerId -> !blockManagerId.equals(managerId))
                                          .collect(Collectors.toSet());
    }

    private RpcEndPointRef getExecutorEndpointRef(String executorId) {
        BlockManagerId id = blockManagerIdByExecutor.get(executorId);
        if (id == null) {
            return null;
        }
        BlockManagerInfo info = blockManagerInfo.get(id);
        if (info == null) {
            return null;
        }
        return info.slaveEndpoint;
    }

    private Map<BlockManagerId, Pair<Long, Long>> memoryStatus() {
        Map<BlockManagerId, Pair<Long, Long>> memoryInfo = Maps.newHashMap();
        blockManagerInfo.forEach((id, info) ->
            memoryInfo.put(id, ImmutablePair.of(info.maxMem, info.remainingMem()))
        );
        return memoryInfo;
    }

    private Future<Map<BlockManagerId, BlockManagerInfo.BlockStatus>> blockStatus(BlockId blockId, boolean askSlaves) {
        GetBlockStatus blockStatus = new GetBlockStatus(blockId);
        throw new UnsupportedOperationException("");
    }

    private void removeExecutor(String execId) {
        LOGGER.info("Trying to remove executor {} from BlockManagerMaster.", execId);
        BlockManagerId blockManagerId = blockManagerIdByExecutor.remove(execId);
        if (blockManagerId != null) {
            removeBlockManager(blockManagerId);
        }
    }

    private void removeBlockManager(BlockManagerId blockManagerId) {
        // 移除BlockManager同时, 移除存在Executor上Block存储数据
        BlockManagerInfo info = blockManagerInfo.remove(blockManagerId);
        blockManagerIdByExecutor.remove(blockManagerId.executorId);

        // 移除数据块信息
        Iterator<BlockId> iterator = info.blocks().keySet().iterator();
        while (iterator.hasNext()) {
            BlockId blockId = iterator.next();
            Set<BlockManagerId> locations = blockLocations.get(blockId);
            locations.remove(blockManagerId);

            if (locations.size() == 0) {
                // Block数据只存储在blockManagerId中(没有数据副本)
                blockLocations.remove(blockId);
                LOGGER.info("No more replicas available for {} !", blockId);
            } else if (proactivelyReplicate && blockId.isRDD()) {
                int maxReplicas = locations.size() + 1;
                int i = (new Random(blockId.hashCode())).nextInt(locations.size());
                BlockManagerId[] blockLocations = locations.toArray(new BlockManagerId[locations.size()]);
                // TODO: 删除副本的主节?
                BlockManagerId candidateBMId = blockLocations[i];
                BlockManagerInfo managerInfo = blockManagerInfo.get(candidateBMId);
                if (managerInfo != null) {
                    List<BlockManagerId> remainingLocations = Lists.newLinkedList();
                    for (int k = 0; k < blockLocations.length; ++k) {
                        if (blockLocations[k].equals(candidateBMId)) {
                            continue;
                        }
                        remainingLocations.add(blockLocations[k]);
                    }
                    BlockManagerId[] replicas = remainingLocations.toArray(new BlockManagerId[remainingLocations.size()]);
                    managerInfo.slaveEndpoint.ask(new ReplicateBlock(blockId, replicas, maxReplicas));
                }
            }

            listenerBus.post(new SparkListenerBlockManagerRemoved(System.currentTimeMillis(), blockManagerId));
            LOGGER.info("Removing block manager {}", blockManagerId);
        }
    }
}
