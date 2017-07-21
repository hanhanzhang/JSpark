package com.sdu.spark.storage;

import com.sdu.spark.utils.ChunkedByteBuffer;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
public class BlockManager {

    public BlockManagerId blockManagerId;

    private BlockInfoManager blockInfoManager = new BlockInfoManager();

    public BlockManagerMaster master;



    public void initialize(String appId) {

    }

    public void reRegister() {

    }

    public List<BlockId> releaseAllLocksForTask(long taskId) {
        return blockInfoManager.releaseAllLocksForTask(taskId);
    }

    public boolean putBytes(BlockId blockId, ChunkedByteBuffer bytes, StorageLevel level) {
        assert bytes != null : "Bytes is null";
        return doPutBytes(blockId, bytes, level, true);
    }


    private boolean doPutBytes(BlockId blockId, ChunkedByteBuffer bytes,
                                   StorageLevel level, boolean tellMaster) {
        throw new UnsupportedOperationException("");
    }

}
