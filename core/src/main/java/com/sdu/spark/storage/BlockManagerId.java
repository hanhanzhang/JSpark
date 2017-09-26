package com.sdu.spark.storage;

import com.google.common.collect.Maps;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.ConcurrentMap;

/**
 * @author hanhan.zhang
 * */
public class BlockManagerId implements Externalizable {
    public String executorId;
    public String host;
    public int port;
    public String topologyInfo;

    private static ConcurrentMap<BlockManagerId, BlockManagerId> blockManagerIdCache;

    static {
        blockManagerIdCache = Maps.newConcurrentMap();
    }

    public BlockManagerId() {
        this(null, null, 0, null);
    }

    public BlockManagerId(String executorId, String host, int port, String topologyInfo) {
        this.executorId = executorId;
        this.host = host;
        this.port = port;
        this.topologyInfo = topologyInfo;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(executorId);
        out.writeUTF(host);
        out.writeInt(port);
        out.writeBoolean(topologyInfo != null);
        out.writeUTF(topologyInfo);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        executorId = in.readUTF();
        host = in.readUTF();
        port = in.readInt();
        boolean isTopologyInfoAvailable = in.readBoolean();
        if (isTopologyInfoAvailable) {
            topologyInfo = in.readUTF();
        } else {
            topologyInfo = null;
        }
    }

    public static BlockManagerId apply(String execId, String host, int port, String topologyInfo) {
        return getCachedBlockManagerId(new BlockManagerId(execId, host, port, topologyInfo));
    }

    public static BlockManagerId apply(ObjectInput in) {
        try {
            BlockManagerId obj = new BlockManagerId();
            obj.readExternal(in);
            return getCachedBlockManagerId(obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static BlockManagerId getCachedBlockManagerId(BlockManagerId id) {
        blockManagerIdCache.putIfAbsent(id, id);
        return blockManagerIdCache.get(id);
    }
}
