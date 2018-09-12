package com.sdu.spark.storage;

/**
 * @author hanhan.zhang
 * */
public class BlockStatus {

    private StorageLevel storageLevel;
    private long memorySize;
    private long diskSize;

    public BlockStatus(StorageLevel storageLevel, long memorySize, long diskSize) {
        this.storageLevel = storageLevel;
        this.memorySize = memorySize;
        this.diskSize = diskSize;
    }

    public StorageLevel getStorageLevel() {
        return storageLevel;
    }

    public long getMemorySize() {
        return memorySize;
    }

    public long getDiskSize() {
        return diskSize;
    }

    public boolean isCached() {
        return memorySize + diskSize > 0;
    }

    // 标识BlockStatus待删除
    public static BlockStatus empty() {
        return new BlockStatus(StorageLevel.NONE, 0L, 0L);
    }


}
