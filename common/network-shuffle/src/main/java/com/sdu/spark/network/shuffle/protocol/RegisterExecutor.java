package com.sdu.spark.network.shuffle.protocol;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.sdu.spark.network.protocol.Encoders;
import io.netty.buffer.ByteBuf;

/**
 * @author hanhan.zhang
 * */
public class RegisterExecutor extends BlockTransferMessage {
    public final String appId;
    public final String execId;
    public final ExecutorShuffleInfo executorInfo;

    public RegisterExecutor(
            String appId,
            String execId,
            ExecutorShuffleInfo executorInfo) {
        this.appId = appId;
        this.execId = execId;
        this.executorInfo = executorInfo;
    }

    @Override
    protected Type type() { return Type.REGISTER_EXECUTOR; }

    @Override
    public int hashCode() {
        return Objects.hashCode(appId, execId, executorInfo);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("appId", appId)
                .add("execId", execId)
                .add("executorInfo", executorInfo)
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof RegisterExecutor) {
            RegisterExecutor o = (RegisterExecutor) other;
            return Objects.equal(appId, o.appId)
                    && Objects.equal(execId, o.execId)
                    && Objects.equal(executorInfo, o.executorInfo);
        }
        return false;
    }

    @Override
    public int encodedLength() {
        return Encoders.Strings.encodedLength(appId)
                + Encoders.Strings.encodedLength(execId)
                + executorInfo.encodedLength();
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.Strings.encode(buf, appId);
        Encoders.Strings.encode(buf, execId);
        executorInfo.encode(buf);
    }

    public static RegisterExecutor decode(ByteBuf buf) {
        String appId = Encoders.Strings.decode(buf);
        String execId = Encoders.Strings.decode(buf);
        ExecutorShuffleInfo executorShuffleInfo = ExecutorShuffleInfo.decode(buf);
        return new RegisterExecutor(appId, execId, executorShuffleInfo);
    }
}
