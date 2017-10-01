package com.sdu.spark.storage;

import com.sdu.spark.network.buffer.ManagedBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author hanhan.zhang
 * */
public class BlockManagerManagedBuffer extends ManagedBuffer {

    private BlockInfoManager blockInfoManager;
    private BlockId blockId;
    private BlockData blockData;
    private boolean dispose;

    private AtomicInteger refCount = new AtomicInteger(1);

    public BlockManagerManagedBuffer(BlockInfoManager blockInfoManager, BlockId blockId,
                                     BlockData blockData, boolean dispose) {
        this.blockInfoManager = blockInfoManager;
        this.blockId = blockId;
        this.blockData = blockData;
        this.dispose = dispose;
    }

    @Override
    public long size() {
        return blockData.size();
    }

    @Override
    public Object convertToNetty() throws IOException {
        return blockData.toNetty();
    }

    @Override
    public ByteBuffer nioByteBuffer() throws IOException {
        return blockData.toByteBuffer();
    }



    @Override
    public ManagedBuffer release() {
        blockInfoManager.unlock(blockId, -1);
        if (refCount.decrementAndGet() == 0 && dispose) {
            blockData.dispose();
        }
        return this;
    }

    @Override
    public ManagedBuffer retain() {
        refCount.incrementAndGet();
        BlockInfo locked = blockInfoManager.lockForReading(blockId, false);
        assert locked != null;
        return this;
    }

    @Override
    public InputStream createInputStream() throws IOException {
        return blockData.toInputStream();
    }
}
