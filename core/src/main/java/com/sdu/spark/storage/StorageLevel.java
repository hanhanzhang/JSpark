package com.sdu.spark.storage;

import com.google.common.collect.Maps;
import com.sdu.spark.SparkException;
import com.sdu.spark.memory.MemoryMode;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

/**
 * @author hanhan.zhang
 * */
public class  StorageLevel implements Externalizable {

    public static final StorageLevel NONE = new StorageLevel(false, false, false, false, 1);
    public static final StorageLevel DISK_ONLY = new StorageLevel(true, false, false, false, 1);
    public static final StorageLevel DISK_ONLY_2 = new StorageLevel(true, false, false, false, 2);
    public static final StorageLevel MEMORY_ONLY = new StorageLevel(false, true, false, true, 1);
    public static final StorageLevel MEMORY_ONLY_2 = new StorageLevel(false, true, false, true, 2);
    public static final StorageLevel MEMORY_ONLY_SER = new StorageLevel(false, true, false, false, 1);
    public static final StorageLevel MEMORY_ONLY_SER_2 = new StorageLevel(false, true, false, false, 2);
    public static final StorageLevel MEMORY_AND_DISK = new StorageLevel(true, true, false, true, 1);
    public static final StorageLevel MEMORY_AND_DISK_2 = new StorageLevel(true, true, false, true, 2);
    public static final StorageLevel MEMORY_AND_DISK_SER = new StorageLevel(true, true, false, false, 1);
    public static final StorageLevel MEMORY_AND_DISK_SER_2 = new StorageLevel(true, true, false, false, 2);
    public static final StorageLevel OFF_HEAP = new StorageLevel(true, true, true, false, 1);

    private static Map<StorageLevel, StorageLevel> storageLevelCache = Maps.newConcurrentMap();


    /** 第4个比特位 */
    private boolean useDisk;
    /** 第3个比特位 */
    private boolean useMemory;
    /** 第2个比特位 */
    private boolean useOffHeap;
    /** 第1个比特位 */
    private boolean deserialized;
    private int replication;

    public StorageLevel() {
        this(false, true, false, false, 1);
    }

    public StorageLevel(int flag, int replication) {
        this((flag & 8) != 0, (flag & 4) != 0, (flag & 2) != 0, (flag & 1) != 0, replication);
    }

    /**
     * @param useDisk: 是否允许写入磁盘
     * @param useMemory: 是否使用堆内存
     * @param useOffHeap: 是否使用堆外内存
     * @param deserialized: 是否Block序列化
     * @param replication: 复制副本
     * */
    public StorageLevel(boolean useDisk, boolean useMemory, boolean useOffHeap, boolean deserialized, int replication) {
        this.useDisk = useDisk;
        this.useMemory = useMemory;
        this.useOffHeap = useOffHeap;
        this.deserialized = deserialized;
        this.replication = replication;

        assert this.replication < 40 : "Replication restricted to be less than 40 for calculating hash codes";
        if (this.useOffHeap) {
            assert !this.deserialized : "Off-heap storage level does not support deserialized storage";
        }
    }

    public boolean isUseDisk() {
        return useDisk;
    }

    public boolean isUseMemory() {
        return useMemory;
    }

    public boolean isUseOffHeap() {
        return useOffHeap;
    }

    public boolean isDeserialized() {
        return deserialized;
    }

    public int getReplication() {
        return replication;
    }

    private int toInt() {
        int flag = 0;
        if (useDisk) {
            flag |= 8;
        }
        if (useMemory) {
            flag |= 4;
        }
        if (useOffHeap) {
            flag |= 2;
        }
        if (deserialized) {
            flag |= 1;
        }
        return flag;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(toInt());
        out.writeByte(replication);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        int flags = in.readByte();
        useDisk = (flags & 8) != 0;
        useMemory = (flags & 4) != 0;
        useOffHeap = (flags & 2) != 0;
        deserialized = (flags & 1) != 0;
        replication = in.readByte();
    }

    @Override
    protected StorageLevel clone() throws CloneNotSupportedException {
        return new StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication);
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof StorageLevel) {
            StorageLevel s = (StorageLevel) object;
            return s.useDisk == useDisk
                    && s.useMemory == useMemory
                    && s.useOffHeap == useOffHeap
                    && s.deserialized == deserialized
                    && s.replication == replication;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return toInt() * 41 + replication;
    }

    public MemoryMode memoryMode() {
        return useOffHeap ? MemoryMode.OFF_HEAP : MemoryMode.ON_HEAP;
    }

    public boolean isValid() {
       return (useMemory || useDisk) && (replication > 0);
    }

    public static StorageLevel fromString(String s){
        switch (s) {
            case "NONE":
                return NONE;
            case "DISK_ONLY" :
                return DISK_ONLY;
            case "DISK_ONLY_2" :
                return DISK_ONLY_2;
            case "MEMORY_ONLY" :
                return MEMORY_ONLY;
            case "MEMORY_ONLY_2" :
                return MEMORY_ONLY_2;
            case "MEMORY_ONLY_SER" :
                return MEMORY_ONLY_SER;
            case "MEMORY_ONLY_SER_2" :
                return MEMORY_ONLY_SER_2;
            case "MEMORY_AND_DISK" :
                return MEMORY_AND_DISK;
            case "MEMORY_AND_DISK_2" :
                return MEMORY_AND_DISK_2;
            case "MEMORY_AND_DISK_SER" :
                return MEMORY_AND_DISK_SER;
            case "MEMORY_AND_DISK_SER_2" :
                return MEMORY_AND_DISK_SER_2;
            case "OFF_HEAP" :
                return OFF_HEAP;
            default:
                throw new IllegalArgumentException("Invalid StorageLevel: " + s);
        }
    }

    public static StorageLevel apply(ObjectInput in) {
        StorageLevel s = new StorageLevel();
        try {
            s.readExternal(in);
        } catch (Exception e) {
            throw new SparkException("Deserialized to StorageLevel failure", e);
        }
        return getCachedStorageLevel(s);
    }

    public static StorageLevel apply(boolean useDisk, boolean useMemory, boolean useOffHeap, boolean deserialized, int replication) {
        return getCachedStorageLevel(new StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication));
    }

    public static StorageLevel apply(int flags, int replication) {
        return getCachedStorageLevel(new StorageLevel(flags, replication));
    }

    private static StorageLevel getCachedStorageLevel(StorageLevel level) {
        storageLevelCache.putIfAbsent(level, level);
        return storageLevelCache.get(level);
    }
}

