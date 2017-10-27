package com.sdu.spark.storage.memory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sdu.spark.memory.MemoryManager;
import com.sdu.spark.memory.MemoryMode;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.serializer.SerializerManager;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.BlockInfoManager;
import com.sdu.spark.storage.StorageLevel;
import com.sdu.spark.utils.ChunkedByteBuffer;
import com.sdu.spark.utils.scala.Either;
import com.sdu.spark.utils.scala.Left;
import com.sdu.spark.utils.scala.Right;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.sdu.spark.utils.Utils.bytesToString;

/**
 * {@link MemoryStore}职责:
 *
 *  1: {@link #entries}记录内存中存储的Block的数据信息
 *
 * todo: putIteratorAsValues、 putIteratorAsBytes、evictBlocksToFreeSpace方法尚未实现
 *
 * @author hanhan.zhang
 * */
public class MemoryStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryStore.class);

    public SparkConf conf;
    private BlockInfoManager blockInfoManager;
    private SerializerManager serializerManager;
    private final MemoryManager memoryManager;
    private BlockEvictionHandler blockEvictionHandler;

    // key = BlockId, value = 存储空间(jvm内存或直接内存)
    private final Map<BlockId, MemoryEntry<?>> entries;
    // key = taskId, value = 存储的jvm空间大小
    private Map<Long, Long> onHeapUnrollMemoryMap;
    // key = taskId, value = 存储的堆外空间大小
    private Map<Long, Long> offHeapUnrollMemoryMap;

    private long unrollMemoryThreshold;

    public MemoryStore(SparkConf conf,
                       BlockInfoManager blockInfoManager,
                       SerializerManager serializerManager,
                       MemoryManager memoryManager,
                       BlockEvictionHandler blockEvictionHandler) {
        this.conf = conf;
        this.blockInfoManager = blockInfoManager;
        this.serializerManager = serializerManager;
        this.memoryManager = memoryManager;
        this.blockEvictionHandler = blockEvictionHandler;

        this.entries = new LinkedHashMap<>(32, 0.75f, true);
        this.onHeapUnrollMemoryMap = Maps.newHashMap();
        this.offHeapUnrollMemoryMap = Maps.newHashMap();

        this.unrollMemoryThreshold = conf.getLong("spark.storage.unrollMemoryThreshold", 1024 * 1024);

        if (maxMemory() < unrollMemoryThreshold) {
            LOGGER.warn("Max memory {} is less than the initialCollection memory threshold {} needed to store a block in memory. " +
                        "Please configure Spark with more memory.", bytesToString(maxMemory()), bytesToString(unrollMemoryThreshold));
        }

        LOGGER.info("MemoryStore started with capacity {}", bytesToString(maxMemory()));
    }

    private long maxMemory() {
        return memoryManager.maxOnHeapStorageMemory() + memoryManager.maxOffHeapStorageMemory();
    }

    public long memoryUsed() {
        return memoryManager.storageMemoryUsed();
    }

    public long blocksMemoryUsed() {
        synchronized (memoryManager) {
            return memoryUsed() - currentUnrollMemory();
        }
    }

    public long getSize(BlockId blockId) {
        synchronized (entries) {
            MemoryEntry entry = entries.get(blockId);
            if (entry != null) {
                return entry.size();
            }
            throw new IllegalArgumentException(String.format("%s not exist", blockId));
        }
    }

    /**
     * Return the amount of memory currently occupied for unrolling blocks across all tasks.
     */
    public long currentUnrollMemory() {
        synchronized (memoryManager) {
            long onHeapUnrollMemorySum = 0L;
            for (Map.Entry<Long, Long> entry : onHeapUnrollMemoryMap.entrySet()) {
                onHeapUnrollMemorySum += entry.getValue();
            }

            long offHeapUnrollMemorySum = 0L;
            for (Map.Entry<Long, Long> entry : offHeapUnrollMemoryMap.entrySet()) {
                offHeapUnrollMemorySum += entry.getValue();
            }

            return onHeapUnrollMemorySum + offHeapUnrollMemorySum;
        }
    }

    public boolean contains(BlockId blockId) {
        synchronized (entries) {
            return entries.containsKey(blockId);
        }
    }

    public boolean putBytes(BlockId blockId,
                                long size,
                                MemoryMode memoryMode,
                                ChunkedByteBufferAllocator allocator) {
        checkArgument(contains(blockId), String.format("Block %s is already present in the MemoryStore", blockId));

        if (memoryManager.acquireStorageMemory(blockId, size, memoryMode)) {
            // 存储空间申请成功
            ChunkedByteBuffer buffer = allocator.toChunkedByteBuffer((int) size);
            assert buffer.size() == size;
            SerializedMemoryEntry memoryEntry = new SerializedMemoryEntry(buffer, memoryMode);
            synchronized (entries) {
                entries.put(blockId, memoryEntry);
            }
            LOGGER.info("Block {} stored as bytes in memory (estimated size {}, free {})",
                    blockId, bytesToString(size), bytesToString(maxMemory() - blocksMemoryUsed()));
            return true;
        }

        return false;
    }

    public Pair<PartiallyUnrolledIterator<?>, Long> putIteratorAsValues(BlockId blockId, Iterator<?> values) {
        throw new UnsupportedOperationException("");
    }



    public ChunkedByteBuffer getBytes(BlockId blockId) {
        MemoryEntry entry;
        synchronized (entries) {
            entry = entries.get(blockId);
        }
        if (entry == null) {
            return null;
        }
        if (entry instanceof DeserializedMemoryEntry) {
            throw new IllegalArgumentException("should only call getBytes on serialized blocks");
        }
        return ((SerializedMemoryEntry) entry).buffer;
    }

    public Iterator<?> getValues(BlockId blockId) {
        MemoryEntry entry;
        synchronized (entries) {
            entry = entries.get(blockId);
        }
        if (entry == null) {
            throw new IllegalArgumentException("should only call getValues on deserialized blocks");
        }
        DeserializedMemoryEntry<?> memoryEntry = (DeserializedMemoryEntry<?>) entry;

        return memoryEntry.array.iterator();
    }

    public boolean remove(BlockId blockId) {
        synchronized (memoryManager) {
            MemoryEntry entry;
            synchronized (entries) {
                entry = entries.remove(blockId);
            }
            if (entry != null) {
                if (entry instanceof SerializedMemoryEntry) {
                    ((SerializedMemoryEntry) entry).buffer.dispose();
                }

                memoryManager.releaseStorageMemory(entry.size(), entry.memoryMode());
                return true;
            }

            return false;
        }
    }

    public void clear() {
        synchronized (memoryManager) {
            synchronized (entries) {
                entries.clear();
            }
            onHeapUnrollMemoryMap.clear();
            offHeapUnrollMemoryMap.clear();
            memoryManager.releaseAllStorageMemory();
        }
    }

    private int getRddId(BlockId blockId) {
        return blockId.asRDDId().rddId;
    }

    private boolean blockIsEvictable(BlockId blockId, MemoryEntry<?> entry, int acquireMemoryRddId, MemoryMode needFreeMemoryModel) {
        return needFreeMemoryModel == entry.memoryMode() &&
                acquireMemoryRddId != getRddId(blockId);
    }

    private <T> void dropBlock(BlockId blockId, MemoryEntry<T> entry) {
        Either<List<T>, ChunkedByteBuffer> data = null;
        if (entry instanceof DeserializedMemoryEntry) {
            DeserializedMemoryEntry<T> deserializedMemoryEntry = (DeserializedMemoryEntry<T>) entry;
            data = new Left<>(deserializedMemoryEntry.array);
        } else if (entry instanceof SerializedMemoryEntry) {
            SerializedMemoryEntry serializedMemoryEntry = (SerializedMemoryEntry) entry;
            data = new Right<>(serializedMemoryEntry.buffer);
        }
        StorageLevel newEffectiveStorageLevel = blockEvictionHandler.dropFromMemory(blockId, data);
        if (newEffectiveStorageLevel.isValid()) {
            // The block is still present in at least one store, so release the lock
            // but don't delete the block info
            blockInfoManager.unlock(blockId);
        } else {
            // The block isn't present in any store, so delete the block info so that the
            // block can be stored again
            blockInfoManager.removeBlock(blockId);
        }
    }

    public long evictBlocksToFreeSpace(BlockId blockId, long space, MemoryMode memoryMode) {
        assert space > 0;
        synchronized (memoryManager) {
            // 已释放内存容量
            long freedMemory = 0L;
            int rddId = getRddId(blockId);

            // 选择可释放内存BlockId
            List<BlockId> selectedBlocks = Lists.newArrayList();
            synchronized (entries) {
                Iterator<BlockId> iterator = entries.keySet().iterator();
                while (iterator.hasNext() && freedMemory < space) {
                    BlockId candidateBlockId = iterator.next();
                    MemoryEntry candidateMemoryEntry = entries.get(candidateBlockId);
                    if (blockIsEvictable(candidateBlockId, candidateMemoryEntry, rddId, memoryMode)) {
                        // 可释放的BlockId需确保无其他进程读取数据
                        if (blockInfoManager.lockForWriting(candidateBlockId, false) != null) {
                            selectedBlocks.add(candidateBlockId);
                            freedMemory += candidateMemoryEntry.size();
                        }
                    }
                }
            }

            // Block数据Spill到磁盘
            if (freedMemory >= space) {
                LOGGER.info("{} blocks selected for dropping {} bytes", selectedBlocks.size(), bytesToString(freedMemory));
                selectedBlocks.forEach(freeBlockId -> {
                    MemoryEntry<?> entry = null;
                    synchronized (entries) {
                        entry = entries.get(freeBlockId);
                    }
                    if (entry != null) {
                        dropBlock(freeBlockId, entry);
                    }
                });
                LOGGER.info("After dropping {} blocks, free memory is {}", selectedBlocks.size(), bytesToString(maxMemory() - blocksMemoryUsed()));
                return freedMemory;
            }
            // 释放内存失败
            LOGGER.info("will not store {}", blockId);
            selectedBlocks.forEach(blockInfoManager::unlock);
            return 0L;
        }
    }

    public void releaseUnrollMemoryForThisTask(MemoryMode memoryMode) {
        releaseUnrollMemoryForThisTask(memoryMode, Long.MAX_VALUE);
    }

    public void releaseUnrollMemoryForThisTask(MemoryMode memoryMode, long memory) {
        // TODO: 待实现
        throw new UnsupportedOperationException("Unsupported");
    }

    public interface ChunkedByteBufferAllocator {
        ChunkedByteBuffer toChunkedByteBuffer(int size);
    }

    public class PartiallyUnrolledIterator<T> implements Iterator<T> {

        private MemoryStore memoryStore;
        private MemoryMode memoryMode;
        private long unrollMemory;
        private Iterator<T> unrolled;
        private Iterator<T> rest;

        public PartiallyUnrolledIterator(MemoryStore memoryStore, MemoryMode memoryMode, long unrollMemory,
                                         Iterator<T> unrolled, Iterator<T> rest) {
            this.memoryStore = memoryStore;
            this.memoryMode = memoryMode;
            this.unrollMemory = unrollMemory;
            this.unrolled = unrolled;
            this.rest = rest;
        }

        @Override
        public boolean hasNext() {
            if (unrolled == null) {
                return rest.hasNext();
            } else if (!unrolled.hasNext()) {
                releaseUnrollMemory();
                return rest.hasNext();
            } else {
                return true;
            }
        }

        @Override
        public T next() {
            if (unrolled == null || !unrolled.hasNext()) {
                return rest.next();
            } else {
                return unrolled.next();
            }
        }

        private void releaseUnrollMemory() {
            memoryStore.releaseUnrollMemoryForThisTask(memoryMode, unrollMemory);
            // SPARK-17503: Garbage collects the unrolling memory before the life end of
            // PartiallyUnrolledIterator.
            unrolled = null;
        }

        public void close() {
            if (unrolled != null) {

            }
        }
    }
}
