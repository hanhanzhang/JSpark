package com.sdu.spark.storage;

import java.util.List;
import java.util.Set;

/**
 *
 * Block数据块副本策略
 *
 * @author hanhan.zhang
 * */
public interface BlockReplicationPolicy {


    /**
     * @param peers : Block副本可存放地址集合
     * @param numReplicas : 存放副本数
     * */
    List<BlockManagerId> prioritize(BlockInfoManager blockInfoManager, Set<BlockManagerId> peers,
                                    BlockId blockId, int numReplicas);
}
