package com.sdu.spark.unfase.memory;


/**
 * Java Object内存地址信息
 *
 * @author hanhan.zhang
 * */
public class MemoryLocation {

    Object obj;

    // 内存地址偏移量
    long offset;

    public MemoryLocation(Object obj, long offset) {
        this.obj = obj;
        this.offset = offset;
    }

    public MemoryLocation() {
        this(null, 0);
    }

    public void setObjAndOffset(Object newObj, long newOffset) {
        this.obj = newObj;
        this.offset = newOffset;
    }

    public final Object getBaseObject() {
        return obj;
    }

    public final long getBaseOffset() {
        return offset;
    }
}
