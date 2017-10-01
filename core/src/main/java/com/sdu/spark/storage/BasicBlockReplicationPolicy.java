package com.sdu.spark.storage;

import java.util.List;
import java.util.Set;

/**
 * @author hanhan.zhang
 * */
public class BasicBlockReplicationPolicy implements BlockReplicationPolicy {

    @Override
    public List<BlockManagerId> prioritize(BlockInfoManager blockInfoManager,
                                           Set<BlockManagerId> peers, BlockId blockId, int numReplicas) {
        return null;
    }
}
