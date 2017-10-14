package com.sdu.spark.shuffle;

import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.storage.BlockId.*;

/**
 *
 * retrieve block data for a logical shuffle block identifier. This is used by the BlockStore to abstract
 * over different shuffle implementations when shuffle data is retrieved.
 *
 * @author hanhan.zhang
 * */
public interface ShuffleBlockResolver {

    ManagedBuffer getBlockData(ShuffleBlockId blockId);

    void stop();

}
