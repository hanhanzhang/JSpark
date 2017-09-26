package com.sdu.spark.storage;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.Serializable;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author hanhan.zhang
 * */
public abstract class BlockId implements Serializable {

    private static Pattern RDD = Pattern.compile("rdd_([0-9]+)_([0-9]+)");
    private static Pattern SHUFFLE = Pattern.compile("shuffle_([0-9]+)_([0-9]+)_([0-9]+)");
    private static Pattern SHUFFLE_DATA = Pattern.compile("shuffle_([0-9]+)_([0-9]+)_([0-9]+).data");
    private static Pattern SHUFFLE_INDEX = Pattern.compile("shuffle_([0-9]+)_([0-9]+)_([0-9]+).index");
    private static Pattern BROADCAST = Pattern.compile("broadcast_([0-9]+)([_A-Za-z0-9]*)");
    private static Pattern TASKRESULT = Pattern.compile("taskresult_([0-9]+)");
    private static Pattern STREAM = Pattern.compile("input-([0-9]+)-([0-9]+)");

    public abstract String name();

    // convenience methods
    public RDDBlockId asRDDId() {
        return isRDD() ? (RDDBlockId) this : null;
    }

    public boolean isRDD() {
        return this instanceof RDDBlockId;
    }

    public boolean isShuffle() {
        return this instanceof ShuffleBlockId;
    }

    public boolean isBroadcast() {
        return this instanceof BroadcastBlockId;
    }

    public static BlockId apply(String id) {
        Matcher m = RDD.matcher(id);
        if (m.find()) {
            return new RDDBlockId(NumberUtils.toInt(m.group(1)), NumberUtils.toInt(m.group(2)));
        }

        m = SHUFFLE.matcher(id);
        if (m.find()) {
            return new ShuffleBlockId(NumberUtils.toInt(m.group(1)),
                                      NumberUtils.toInt(m.group(2)),
                                      NumberUtils.toInt(m.group(3)));
        }

        m = SHUFFLE_DATA.matcher(id);
        if (m.find()) {
            return new ShuffleDataBlockId(NumberUtils.toInt(m.group(1)),
                                          NumberUtils.toInt(m.group(2)),
                                          NumberUtils.toInt(m.group(3)));
        }

        m = SHUFFLE_INDEX.matcher(id);
        if (m.find()) {
            return new ShuffleIndexBlockId(NumberUtils.toInt(m.group(1)),
                                           NumberUtils.toInt(m.group(2)),
                                           NumberUtils.toInt(m.group(3)));
        }

        m = BROADCAST.matcher(id);
        if (m.find()) {
            return new BroadcastBlockId(NumberUtils.toLong(m.group(1)),
                                        m.group(2));
        }

        m = TASKRESULT.matcher(id);
        if (m.find()) {
            return new TaskResultBlockId(NumberUtils.toLong(m.group(1)));
        }

        m = STREAM.matcher(id);
        if (m.find()) {
            return new StreamBlockId(NumberUtils.toInt(m.group(1)),
                                     NumberUtils.toInt(m.group(2)));
        }

        throw new IllegalStateException("Unrecognized BlockId: " + id);
    }

    @Override
    public String toString() {
        return name();
    }

    @Override
    public int hashCode() {
        return name().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BlockId) {
            return obj.getClass() == getClass() && name().equals(((BlockId) obj).name());
        }
        return false;
    }


    public static class RDDBlockId extends BlockId {

        public int rddId;
        public int splitIndex;

        public RDDBlockId(int rddId, int splitIndex) {
            this.rddId = rddId;
            this.splitIndex = splitIndex;
        }

        @Override
        public String name() {
            return String.format("rdd_%d_%d", rddId, splitIndex);
        }
    }

    public static class ShuffleBlockId extends BlockId {
        public int shuffleId;
        public int mapId;
        public int reduceId;

        public ShuffleBlockId(int shuffleId, int mapId, int reduceId) {
            this.shuffleId = shuffleId;
            this.mapId = mapId;
            this.reduceId = reduceId;
        }

        @Override
        public String name() {
            return String.format("shuffle_%d_%d_%d", shuffleId, mapId, reduceId);
        }
    }

    public static class ShuffleDataBlockId extends BlockId {

        public int shuffleId;
        public int mapId;
        public int reduceId;

        public ShuffleDataBlockId(int shuffleId, int mapId, int reduceId) {
            this.shuffleId = shuffleId;
            this.mapId = mapId;
            this.reduceId = reduceId;
        }

        @Override
        public String name() {
            return String.format("shuffle_%d_%d_%d.data", shuffleId, mapId, reduceId);
        }
    }

    public static class ShuffleIndexBlockId extends BlockId {

        public int shuffleId;
        public int mapId;
        public int reduceId;

        public ShuffleIndexBlockId(int shuffleId, int mapId, int reduceId) {
            this.shuffleId = shuffleId;
            this.mapId = mapId;
            this.reduceId = reduceId;
        }

        @Override
        public String name() {
            return String.format("shuffle_%d_%d_%d.index", shuffleId, mapId, reduceId);
        }
    }

    public static class BroadcastBlockId extends BlockId {
        public long broadcastId;
        public String field;

        public BroadcastBlockId(long broadcastId, String field) {
            this.broadcastId = broadcastId;
            this.field = field;
        }

        @Override
        public String name() {
            StringBuilder sb = new StringBuilder();
            sb.append("broadcast_").append(broadcastId);
            if (StringUtils.isNotEmpty(field)) {
                sb.append("_").append(field);
            }
            return sb.toString();
        }
    }

    public static class TaskResultBlockId extends BlockId {

        public long taskId;

        public TaskResultBlockId(long taskId) {
            this.taskId = taskId;
        }

        @Override
        public String name() {
            return String.format("taskresult_%d", taskId);
        }
    }

    public static class StreamBlockId extends BlockId {
        public int streamId;
        public long uniqueId;

        public StreamBlockId(int streamId, long uniqueId) {
            this.streamId = streamId;
            this.uniqueId = uniqueId;
        }

        @Override
        public String name() {
            return String.format("input-%d-%d", streamId, uniqueId);
        }
    }

    public static class TempLocalBlockId extends BlockId {
        public UUID id;

        public TempLocalBlockId(UUID id) {
            this.id = id;
        }

        @Override
        public String name() {
            return String.format("temp_local_%s", id);
        }
    }

    public static class TempShuffleBlockId extends BlockId {
        public UUID id;

        public TempShuffleBlockId(UUID id) {
            this.id = id;
        }

        @Override
        public String name() {
            return String.format("temp_shuffle_%s", id);
        }
    }

}
