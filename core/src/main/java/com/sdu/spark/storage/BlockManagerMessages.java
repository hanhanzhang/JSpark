package com.sdu.spark.storage;

import com.sdu.spark.rpc.RpcEndpointRef;

import java.io.*;

/**
 * 声明Block管理Rpc消息
 *
 * @author hanhan.zhang
 * */
public interface BlockManagerMessages extends Serializable{

    interface ToBlockManagerSlave extends Serializable {}

    class RemoveBlock implements ToBlockManagerSlave {
        public BlockId blockId;

        public RemoveBlock(BlockId blockId) {
            this.blockId = blockId;
        }
    }

    // Replicate blocks that were lost due to executor failure
    class ReplicateBlock implements ToBlockManagerSlave {
        public BlockId blockId;
        public BlockManagerId[] replicas;
        public int maxReplicas;

        public ReplicateBlock(BlockId blockId, BlockManagerId[] replicas, int maxReplicas) {
            this.blockId = blockId;
            this.replicas = replicas;
            this.maxReplicas = maxReplicas;
        }
    }

    // Remove all blocks belonging to a specific RDD.
    class RemoveRdd implements ToBlockManagerSlave {
        public int rddId;

        public RemoveRdd(int rddId) {
            this.rddId = rddId;
        }
    }

    // Remove all blocks belonging to a specific shuffle.
    class RemoveShuffle implements ToBlockManagerSlave {
        public int shuffleId;

        public RemoveShuffle(int shuffleId) {
            this.shuffleId = shuffleId;
        }
    }

    // Remove all blocks belonging to a specific broadcast.
    class RemoveBroadcast implements ToBlockManagerSlave {
        public long broadcastId;
        public boolean removeFromDriver;

        public RemoveBroadcast(long broadcastId) {
            this(broadcastId, true);
        }

        public RemoveBroadcast(long broadcastId, boolean removeFromDriver) {
            this.broadcastId = broadcastId;
            this.removeFromDriver = removeFromDriver;
        }
    }

    class TriggerThreadDump implements ToBlockManagerSlave {}

    /**********************************************************************/

    interface ToBlockManagerMaster extends Serializable {}

    class RegisterBlockManager implements ToBlockManagerMaster {
        public BlockManagerId blockManagerId;
        public long maxOnHeapMemSize;
        public long maxOffHeapMemSize;
        public RpcEndpointRef sender;

        public RegisterBlockManager(BlockManagerId blockManagerId, long maxOnHeapMemSize,
                                    long maxOffHeapMemSize, RpcEndpointRef sender) {
            this.blockManagerId = blockManagerId;
            this.maxOnHeapMemSize = maxOnHeapMemSize;
            this.maxOffHeapMemSize = maxOffHeapMemSize;
            this.sender = sender;
        }
    }

    class UpdateBlockInfo implements ToBlockManagerMaster, Externalizable {
        public BlockManagerId blockManagerId;
        public BlockId blockId;
        public StorageLevel storageLevel;
        public long memSize;
        public long diskSize;

        public UpdateBlockInfo(BlockManagerId blockManagerId, BlockId blockId,
                               StorageLevel storageLevel, long memSize, long diskSize) {
            this.blockManagerId = blockManagerId;
            this.blockId = blockId;
            this.storageLevel = storageLevel;
            this.memSize = memSize;
            this.diskSize = diskSize;
        }

        public UpdateBlockInfo() {
            this(null, null, null, 0, 0);
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            blockManagerId.writeExternal(out);
            out.writeUTF(blockId.name());
            storageLevel.writeExternal(out);
            out.writeLong(memSize);
            out.writeLong(diskSize);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            blockManagerId = BlockManagerId.apply(in);
            blockId = BlockId.apply(in.readUTF());
            storageLevel = StorageLevel.apply(in);
            memSize = in.readLong();
            diskSize = in.readLong();
        }
    }

    class GetLocations implements ToBlockManagerMaster {
        public BlockId blockId;

        public GetLocations(BlockId blockId) {
            this.blockId = blockId;
        }
    }

    class GetLocationsMultipleBlockIds implements ToBlockManagerMaster {
        public BlockId[] blockIds;

        public GetLocationsMultipleBlockIds(BlockId[] blockIds) {
            this.blockIds = blockIds;
        }
    }

    class GetPeers implements ToBlockManagerMaster {
        public BlockManagerId blockManagerId;

        public GetPeers(BlockManagerId blockManagerId) {
            this.blockManagerId = blockManagerId;
        }
    }

    class GetExecutorEndpointRef implements ToBlockManagerMaster {
        public String execId;

        public GetExecutorEndpointRef(String execId) {
            this.execId = execId;
        }
    }

    class RemoveExecutor implements ToBlockManagerMaster {
        public String execId;

        public RemoveExecutor(String execId) {
            this.execId = execId;
        }
    }

    class StopBlockManagerMaster implements ToBlockManagerMaster{}

    class GetMemoryStatus implements ToBlockManagerMaster {}

    class GetStorageStatus implements ToBlockManagerMaster {}

    class GetBlockStatus implements ToBlockManagerMaster {
        public BlockId blockId;
        public boolean askSlave;

        public GetBlockStatus(BlockId blockId) {
            this(blockId, true);
        }

        public GetBlockStatus(BlockId blockId, boolean askSlave) {
            this.blockId = blockId;
            this.askSlave = askSlave;
        }
    }

    class GetMatchingBlockIds implements ToBlockManagerMaster {
        public MatchingBlockFilter blockFilter;
        public boolean askSlave;

        public GetMatchingBlockIds(MatchingBlockFilter blockFilter) {
            this(blockFilter, true);
        }

        public GetMatchingBlockIds(MatchingBlockFilter blockFilter, boolean askSlave) {
            this.blockFilter = blockFilter;
            this.askSlave = askSlave;
        }
    }

    class BlockManagerHeartbeat implements ToBlockManagerMaster {
        public BlockManagerId blockManagerId;

        public BlockManagerHeartbeat(BlockManagerId blockManagerId) {
            this.blockManagerId = blockManagerId;
        }
    }

    class HasCachedBlocks implements ToBlockManagerMaster {
        public String executorId;

        public HasCachedBlocks(String executorId) {
            this.executorId = executorId;
        }
    }

    interface MatchingBlockFilter extends Serializable {
        boolean filter(BlockId blockId);
    }

}
