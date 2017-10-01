package com.sdu.spark.network.shuffle.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.sdu.spark.network.protocol.Encodable;
import com.sdu.spark.network.protocol.Encoders;
import io.netty.buffer.ByteBuf;

import java.util.Arrays;

/**
 * @author hanhan.zhang
 * */
public class ExecutorShuffleInfo implements Encodable {

    /** The base set of local directories that the executor stores its shuffle files in. */
    public final String[] localDirs;
    /** Number of subdirectories created within each localDir. */
    public final int subDirsPerLocalDir;
    /** Shuffle manager (SortShuffleManager) that the executor is using. */
    public final String shuffleManager;

    @JsonCreator
    public ExecutorShuffleInfo(@JsonProperty("localDirs") String[] localDirs,
                               @JsonProperty("subDirsPerLocalDir") int subDirsPerLocalDir,
                               @JsonProperty("shuffleManager") String shuffleManager) {
        this.localDirs = localDirs;
        this.subDirsPerLocalDir = subDirsPerLocalDir;
        this.shuffleManager = shuffleManager;
    }

    @Override
    public int encodedLength() {
        return Encoders.StringArrays.encodedLength(localDirs)
                + 4 // int
                + Encoders.Strings.encodedLength(shuffleManager);
    }

    @Override
    public void encode(ByteBuf buf) {
        Encoders.StringArrays.encode(buf, localDirs);
        buf.writeInt(subDirsPerLocalDir);
        Encoders.Strings.encode(buf, shuffleManager);
    }

    public static ExecutorShuffleInfo decode(ByteBuf buf) {
        String[] localDirs = Encoders.StringArrays.decode(buf);
        int subDirsPerLocalDir = buf.readInt();
        String shuffleManager = Encoders.Strings.decode(buf);
        return new ExecutorShuffleInfo(localDirs, subDirsPerLocalDir, shuffleManager);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(subDirsPerLocalDir, shuffleManager) * 41 + Arrays.hashCode(localDirs);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("localDirs", Arrays.toString(localDirs))
                .add("subDirsPerLocalDir", subDirsPerLocalDir)
                .add("shuffleManager", shuffleManager)
                .toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other != null && other instanceof ExecutorShuffleInfo) {
            ExecutorShuffleInfo o = (ExecutorShuffleInfo) other;
            return Arrays.equals(localDirs, o.localDirs)
                    && Objects.equal(subDirsPerLocalDir, o.subDirsPerLocalDir)
                    && Objects.equal(shuffleManager, o.shuffleManager);
        }
        return false;
    }
}
