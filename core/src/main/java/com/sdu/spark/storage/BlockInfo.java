package com.sdu.spark.storage;

/**
 * {@link BlockInfo}标识Block数据块读写状态
 *
 * @author hanhan.zhang
 * */
public class BlockInfo {

    // Special task attempt id constant used to mark a block's write lock as being unlocked.
    public static final long NO_WRITER = -1;

    // Special task attempt id constant used to mark a block's write lock as being held by
    // a non-task thread (e.g. by a driver thread or by unit test code)
    // 标识Block被"非任务"线程标记写(如Driver线程)
    public static final long NON_TASK_WRITER = -1024;

    public StorageLevel storageLevel;
    public boolean tellMaster;

    // Block's size
    public long size;
    // The number of times that this block has been locked for reading.
    public int readerCount;
    /**
     * The task attempt id of the task which currently holds the write lock for this block, or
     * [[BlockInfo.NON_TASK_WRITER]] if the write lock is held by non-task code, or
     * [[BlockInfo.NO_WRITER]] if this block is not locked for writing.
     *
     * Note:
     *    writeTask = taskId
     * */
    public long writerTask = NO_WRITER;

    public BlockInfo(StorageLevel storageLevel, boolean tellMaster) {
        this.storageLevel = storageLevel;
        this.tellMaster = tellMaster;
        checkInvariants();
    }

    private void checkInvariants() {
        // A block's reader count must be non-negative:
        assert readerCount >= 0;
        // A block is either locked for reading or for writing, but not for both at the same time(不能同时读写):
        assert readerCount == 0 || writerTask == BlockInfo.NO_WRITER;
    }

    public int readerCount() {
        return readerCount;
    }

    public void readerCount(int c) {
        readerCount = c;
        checkInvariants();
    }

    public long writerTask() {
        return writerTask;
    }

    public void writerTask(long t) {
        writerTask = t;
        checkInvariants();
    }

    public long size() {
        return size;
    }

    public void size(long s) {
        size = s;
        checkInvariants();
    }
}
