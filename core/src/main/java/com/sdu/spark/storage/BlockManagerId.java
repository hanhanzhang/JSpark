package com.sdu.spark.storage;

import com.google.common.base.Strings;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author hanhan.zhang
 * */
public class BlockManagerId implements Externalizable {
    public String executorId;
    public String host;
    public int port;
    public String topologyInfo;

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
}
