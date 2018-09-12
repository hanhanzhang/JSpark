package com.sdu.spark.storage;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.sdu.spark.rpc.RpcEndpointRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

import static com.sdu.spark.utils.Utils.bytesToString;

/**
 * BlockManagerInfo管理BlockManger数据块存储信息.
 *
 * BlockManagerInfo对Block块管理采用共享锁和排它锁, 读锁共享, 写锁互斥.
 *
 * @author hanhan.zhang
 * */
public class BlockManagerInfo {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlockManagerInfo.class);

    private BlockManagerId blockManagerId;
    private long maxOnHeapMem;
    private long maxOffHeapMem;
    public RpcEndpointRef slaveEndpoint;
    private long lastSeenMs;

    // Executor或Driver分配的存储最大内存
    public long maxMem;
    // Executor或Driver可用存储内存空间
    private long remainingMem;

    // 维护BlockId存储信息
    private Map<BlockId, BlockStatus> blocks;
    private Set<BlockId> cachedBlocks;

    public BlockManagerInfo(BlockManagerId blockManagerId,
                            long maxOnHeapMem,
                            long maxOffHeapMem,
                            RpcEndpointRef slaveEndpoint,
                            long timeMs) {
        this.blockManagerId = blockManagerId;
        this.maxOnHeapMem = maxOnHeapMem;
        this.maxOffHeapMem = maxOffHeapMem;
        this.slaveEndpoint = slaveEndpoint;
        this.lastSeenMs = timeMs;

        this.maxMem = maxOnHeapMem + maxOffHeapMem;
        this.remainingMem = this.maxMem;
        this.blocks = Maps.newHashMap();
        this.cachedBlocks = Sets.newHashSet();
    }

    public BlockStatus getStatus(BlockId blockId) {
        return blocks.get(blockId);
    }

    public void updateLastSeenMs() {
        this.lastSeenMs = System.currentTimeMillis();
    }

    public void updateBlockInfo(BlockId blockId, StorageLevel storageLevel, long memorySize, long diskSize) {
        BlockStatus originBlockStatus = getStatus(blockId);
        if (originBlockStatus != null) {
            // BlockId的数据块已被记录
            StorageLevel originStorageLevel = originBlockStatus.getStorageLevel();
            if (originStorageLevel.isUseMemory()) {
                remainingMem += originBlockStatus.getMemorySize();
            }
        }

        if (storageLevel.isValid()) {
            BlockStatus blockStatus = null;
            if (storageLevel.isUseMemory()) {
                blockStatus = new BlockStatus(storageLevel, memorySize, 0);
                remainingMem -= memorySize;
                blocks.put(blockId, blockStatus);

                // 打印日志
                if (originBlockStatus != null) {
                    LOGGER.info("Updated {} in memory on {} (current size: {}, original size: {}, free: {})",
                            blockId, blockManagerId.hostPort(), bytesToString(originBlockStatus.getMemorySize()),
                            bytesToString(memorySize), bytesToString(remainingMem));
                } else {
                    LOGGER.info("Updated {} in memory on {} (size: {}, free: {})", blockId,
                            blockManagerId.hostPort(), bytesToString(memorySize), bytesToString(remainingMem));
                }
            }

            if (storageLevel.isUseDisk()) {
                blockStatus = new BlockStatus(storageLevel, 0, diskSize);
                blocks.put(blockId, blockStatus);
                if (originBlockStatus != null) {
                    LOGGER.info("Updated {} on disk on {} (current size: {}, original size: {})", blockId, blockManagerId.hostPort(),
                            bytesToString(diskSize), bytesToString(originBlockStatus.getDiskSize()));
                } else {
                    LOGGER.info("Added {} on disk on {} (size: {})", blockId, blockManagerId.hostPort(), bytesToString(diskSize));
                }
            }

            if (!blockId.isBroadcast() && blockStatus != null && blockStatus.isCached()) {
                cachedBlocks.add(blockId);
            }
        } else if (originBlockStatus != null){
            // 丢弃BlockId
            // TODO: 为啥没有重新计算remainMem
            blocks.remove(blockId);
            cachedBlocks.remove(blockId);
            if (originBlockStatus.getStorageLevel().isUseMemory()) {
                LOGGER.info("Removed {} on {} in memory (size: {}, free: {})", blockId, blockManagerId.hostPort(),
                        bytesToString(originBlockStatus.getMemorySize()), bytesToString(remainingMem));
            }
            if (originBlockStatus.getStorageLevel().isUseDisk()) {
                LOGGER.info("Removed {} on {} on disk (size: {})", blockId, blockManagerId.hostPort(),
                        bytesToString(originBlockStatus.getDiskSize()));
            }
        }
    }

    public void removeBlock(BlockId blockId) {
        if (blocks.containsKey(blockId)) {
            remainingMem += blocks.get(blockId).getMemorySize();
            blocks.remove(blockId);
        }
        cachedBlocks.remove(blockId);
    }

    public long remainingMem() {
        return remainingMem;
    }

    public long lastSeenMs() {
        return lastSeenMs;
    }

    public Map<BlockId, BlockStatus> blocks() {
        return blocks;
    }

    // This does not include broadcast blocks.
    public Set<BlockId> cachedBlocks() {
        return cachedBlocks;
    }

    public void clear() {
        blocks.clear();
    }

    @Override
    public String toString() {
        return "BlockManagerInfo " + lastSeenMs + " " + remainingMem;
    }
}
