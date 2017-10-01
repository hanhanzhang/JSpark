package com.sdu.spark.storage;

import com.sdu.spark.memory.MemoryMode;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author hanhan.zhang
 * */
public enum  StorageLevel implements Externalizable {

    NONE(false, false, false, false, 1),
    DISK_ONLY(true, false, false, false, 1),
    DISK_ONLY_2(true, false, false, false, 2),
    MEMORY_ONLY(false, true, false, true, 1),
    MEMORY_ONLY_2(false, true, false, true, 2),
    MEMORY_ONLY_SER(false, true, false, false, 1),
    MEMORY_ONLY_SER_2(false, true, false, false, 2),
    MEMORY_AND_DISK(true, true, false, true, 1),
    MEMORY_AND_DISK_2(true, true, false, true, 2),
    MEMORY_AND_DISK_SER(true, true, false, false, 1),
    MEMORY_AND_DISK_SER_2(true, true, false, false, 2),
    OFF_HEAP(true, true, true, false, 1);


    public boolean useDisk;
    public boolean useMemory;
    public boolean useOffHeap;
    public boolean deserialized;
    public int replication = 1;

    private StorageLevel(boolean useDisk, boolean useMemory, boolean useOffHeap,
                        boolean deserialized, int replication) {
        this.useDisk = useDisk;
        this.useMemory = useMemory;
        this.useOffHeap = useOffHeap;
        this.deserialized = deserialized;
        this.replication = replication;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }

    public MemoryMode memoryMode() {
        if (useOffHeap)
            return MemoryMode.OFF_HEAP;
        return MemoryMode.ON_HEAP;
    }

    public boolean isValid() {
       return (useMemory || useDisk) && (replication > 0);
    }

    public static StorageLevel apply(ObjectInput in) {
        throw new UnsupportedOperationException("");
    }

    public static StorageLevel apply(boolean useDisk, boolean useMemory, boolean useOffHeap,
                                     boolean deserialized, int replication) {
        throw new UnsupportedOperationException("");
    }


}

