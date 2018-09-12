package com.sdu.spark.storage;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.sdu.spark.SparkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.format;

/**
 *
 * {@link BlockInfoManager}负责数据块读写控制, BlockInfo有两种状态标记数据块读取状态:
 *
 *  1: {@link BlockInfo#readerCount} : 标记当前读取数据块Task数
 *
 *  2: {@link BlockInfo#writerTask} :  标记当前读取数据块TaskID
 *
 * @author hanhan.zhang
 * */
public class BlockInfoManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlockInfoManager.class);

    private Map<BlockId, BlockInfo> infos;
    // key = taskId, value = 持有写锁的数据块集合[保证线程安全]
    private Map<Long, Set<BlockId>> writeLocksByTask;
    // key = taskId, value = 持有读锁的数据块集合[保证线程安全]
    private Map<Long, ConcurrentHashMultiset<BlockId>> readLocksByTask;

    public BlockInfoManager() {
        infos = Maps.newHashMap();
        writeLocksByTask = Maps.newHashMap();
        readLocksByTask = Maps.newHashMap();
        registerTask(BlockInfo.NON_TASK_WRITER);
    }

    public synchronized void registerTask(long taskAttemptId) {
        checkArgument(!readLocksByTask.containsKey(taskAttemptId),
                        format("Task attempt %s is already registered", taskAttemptId));
        readLocksByTask.put(taskAttemptId, ConcurrentHashMultiset.create());
    }

    public long currentTaskAttemptId() {
        // TODO:
        throw new UnsupportedOperationException("");
    }

    /**
     * @param blocking true: 阻塞直至获取读锁
     * */
    public synchronized BlockInfo lockForReading(BlockId blockId, boolean blocking) {
        LOGGER.trace("Task {} trying to acquire read lock for {}", currentTaskAttemptId(), blockId);
        do {
            BlockInfo blockInfo = infos.get(blockId);
            if (blockInfo == null) {
                return null;
            }
            // 没有写锁
            if (blockInfo.writerTask() == BlockInfo.NO_WRITER) {
                blockInfo.readerCount(1, true);
                ConcurrentHashMultiset<BlockId> readingBlockIds = readLocksByTask.get(currentTaskAttemptId());
                if (readingBlockIds == null) {
                    readingBlockIds = ConcurrentHashMultiset.create();
                    readLocksByTask.put(currentTaskAttemptId(), readingBlockIds);
                }
                readingBlockIds.add(blockId);
                LOGGER.info("Task {} acquired read lock for {}", currentTaskAttemptId(), blockId);
                return blockInfo;
            }

            if (blocking) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        } while (blocking);

        return null;
    }

    public synchronized BlockInfo lockForReading(BlockId blockId) {
        return lockForReading(blockId, true);
    }

    public synchronized BlockInfo lockForWriting(BlockId blockId, boolean blocking) {
        LOGGER.trace("Task {} trying to acquire write lock for {}", currentTaskAttemptId(), blockId);
        do {
            BlockInfo blockInfo = infos.get(blockId);
            if (blockInfo == null) {
                return null;
            }
            // 数据块没有读写
            if (blockInfo.readerCount() == 0 && blockInfo.writerTask() == BlockInfo.NO_WRITER) {
                blockInfo.writerTask(currentTaskAttemptId());
                Set<BlockId> writingBlockIds =  writeLocksByTask.computeIfAbsent(currentTaskAttemptId(), key -> newHashSet());
                writingBlockIds.add(blockId);
                return blockInfo;
            }

            if (blocking) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        } while (blocking);
        return null;
    }

    public synchronized BlockInfo lockForWriting(BlockId blockId) {
        return lockForWriting(blockId, true);
    }

    public synchronized BlockInfo assertBlockIsLockedForWriting(BlockId blockId) throws SparkException {
        BlockInfo blockInfo = infos.get(blockId);
        if (blockInfo == null) {
            throw new SparkException(format("Block %s does not exist", blockId));
        }

        if (blockInfo.writerTask() != currentTaskAttemptId()) {
            throw new SparkException(format("Task %s has not locked block $blockId for writing",
                                        currentTaskAttemptId()));
        }
        return blockInfo;
    }

    private synchronized BlockInfo get(BlockId blockId) {
        return infos.get(blockId);
    }

    /**
     * Downgrades an exclusive write lock to a shared read lock.
     */
    public synchronized void downgradeLock(BlockId blockId) {
        LOGGER.trace("Task {} downgrading write lock for {}", currentTaskAttemptId(), blockId);
        BlockInfo info = get(blockId);
        checkArgument(info.writerTask() == currentTaskAttemptId(),
                format("Task %s tried to downgrade a write lock that it does not hold on  block %s",
                                currentTaskAttemptId(), blockId));
        unlock(blockId, currentTaskAttemptId());
        BlockInfo lockOutcome = lockForReading(blockId, false);
        assert lockOutcome != null;
    }

    public synchronized void unlock(BlockId blockId) {
        unlock(blockId, currentTaskAttemptId());
    }

    /**
     * Release a lock on the given block.
     * In case a TaskContext is not propagated properly to all child threads for the task, we fail to
     * get the TID from TaskContext, so we have to explicitly pass the TID value to release the lock.
     *
     * See SPARK-18406 for more discussion of this issue.
     */
    public synchronized void unlock(BlockId blockId, long taskId) {
        if (taskId < 0) {
            taskId = currentTaskAttemptId();
        }
        LOGGER.trace("Task {} releasing lock for {}", taskId, blockId);
        BlockInfo blockInfo = get(blockId);
        if (blockInfo == null) {
            throw new IllegalStateException(format("Block %s not found", blockId));
        }

        if (blockInfo.writerTask() != BlockInfo.NO_WRITER) {
            blockInfo.writerTask(BlockInfo.NO_WRITER);
            writeLocksByTask.remove(taskId);
        } else {
            checkArgument(blockInfo.readerCount() > 0, format("Block %s is not locked for reading", blockId));
            blockInfo.readerCount(1, false);
            ConcurrentHashMultiset<BlockId> countsForTask = readLocksByTask.get(taskId);
            int newPinCountForTask = countsForTask.remove(blockId, 1) - 1;
            assert newPinCountForTask >= 0 :
                    format("Task %s release lock on block $blockId more times than it acquired it", taskId);
        }
        notifyAll();
    }

    /**
     * Attempt to acquire the appropriate lock for writing a new block.
     *
     * This enforces the first-writer-wins semantics. If we are the first to write the block,
     * then just go ahead and acquire the write lock. Otherwise, if another thread is already
     * writing the block, then we wait for the write to finish before acquiring the read lock.
     *
     * @return true if the block did not already exist, false otherwise. If this returns false, then
     *         a read lock on the existing block will be held. If this returns true, a write lock on
     *         the new block will be held.
     */
    public synchronized boolean lockNewBlockForWriting(BlockId blockId, BlockInfo newBlockInfo) {
        LOGGER.trace("Task {} trying to put {}", currentTaskAttemptId(), blockId);
        BlockInfo blockInfo = lockForReading(blockId);
        if (blockInfo == null) {
            // Block does not yet exist or is removed, so we are free to acquire the write lock
            infos.put(blockId, newBlockInfo);
            lockForWriting(blockId);
            return true;
        }

        // Block already exists. This could happen if another thread races with us to compute
        // the same block. In this case, just keep the read lock and return.
        return false;
    }

    /**
     * Release all lock held by the given task, clearing that task's pin bookkeeping
     * structures and updating the global pin counts. This method should be called at the
     * end of a task (either by a task completion handler or in `TaskRunner.run()`).
     *
     * @return the ids of blocks whose pins were released
     */
    public List<BlockId> releaseAllLocksForTask(long taskId) {
        List<BlockId> blocksWithReleasedLocks = Lists.newLinkedList();

        ConcurrentHashMultiset<BlockId> readLocks = readLocksByTask.remove(taskId);
        Set<BlockId> writeLocks = writeLocksByTask.remove(taskId);

        writeLocks.forEach(blockId -> {
            BlockInfo blockInfo = infos.get(blockId);
            assert blockInfo.writerTask() == taskId;
            blockInfo.writerTask(BlockInfo.NO_WRITER);
            blocksWithReleasedLocks.add(blockId);
        });

        readLocks.entrySet().forEach(entry -> {
            BlockId blockId = entry.getElement();
            blocksWithReleasedLocks.add(blockId);
            int lockCount = entry.getCount();
            BlockInfo blockInfo = infos.get(blockId);
            blockInfo.readerCount(lockCount, false);
            assert blockInfo.readerCount() >= 0;
        });

        notifyAll();
        return blocksWithReleasedLocks;
    }

    public synchronized int size() {
        return infos.size();
    }

    /**
     * Removes the given block and releases the write lock on it.
     *
     * This can only be called while holding a write lock on the given block.
     */
    public synchronized void removeBlock(BlockId blockId) {
        LOGGER.trace("Task {} trying to remove block {}", currentTaskAttemptId(), blockId);
        BlockInfo blockInfo  = infos.get(blockId);
        if (blockInfo == null) {
            return;
        }
        if (blockInfo.writerTask() == currentTaskAttemptId()) {
            blockInfo.writerTask(BlockInfo.NO_WRITER);
            // 置零
            blockInfo.readerCount(blockInfo.readerCount(), false);
            writeLocksByTask.remove(currentTaskAttemptId());
        }
        notifyAll();
    }

    public synchronized void clear() {
        infos.values().forEach(blockInfo -> {
            blockInfo.readerCount(blockInfo.readerCount(), false);
            blockInfo.writerTask(BlockInfo.NO_WRITER);
        });
        infos.clear();
        readLocksByTask.clear();
        writeLocksByTask.clear();
        notifyAll();
    }

    public Set<Map.Entry<BlockId, BlockInfo>> entries() {
        return infos.entrySet();
    }
}
