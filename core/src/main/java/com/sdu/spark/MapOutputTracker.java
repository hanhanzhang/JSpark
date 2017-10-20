package com.sdu.spark;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.sdu.spark.broadcast.Broadcast;
import com.sdu.spark.broadcast.BroadcastManager;
import com.sdu.spark.rpc.RpcEndPointRef;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.scheduler.MapStatus;
import com.sdu.spark.shuffle.FetchFailedException.MetadataFetchFailedException;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.BlockId.ShuffleBlockId;
import com.sdu.spark.storage.BlockManagerId;
import com.sdu.spark.utils.scala.Tuple2;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * {@link MapOutputTracker}
 *
 * @author hanhan.zhang
 * */
public abstract class MapOutputTracker {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapOutputTracker.class);

    public static final String ENDPOINT_NAME = "MapOutputTracker";
    public static final int DIRECT = 0;
    public static final int BROADCAST = 1;

    /**
     * Driver: MapOutputTrackerMasterEndPoint节点引用
     *
     * Executor: Driver RpcEnv MapOutputTrackerMasterEndPoint节点引用
     * */
    public RpcEndPointRef trackerEndpoint;

    /**
     * 当Shuffle的结果输出失效时, Driver会更新epoch值并将此值作为Task一部分发送给Executor, Executor根据
     * 持有epoch值与new epoch做比较, 若是new epoch值比Executor持有epoch值大, 则清空持有epoch对应的Shuffle
     * 的结果输出
     * */
    protected long epoch = 0L;
    protected Object epochLock = new Object();

    protected SparkConf conf;

    public MapOutputTracker(SparkConf conf) {
        this.conf = conf;
    }

    protected Object askTracker(Object message) throws SparkException{
        try {
            return trackerEndpoint.askSync(message);
        } catch (Exception e) {
            LOGGER.error("Error communicating with MapOutputTracker", e);
            throw new SparkException("Error communicating with MapOutputTracker", e);
        }
    }

    protected void sendTracker(Object message) throws SparkException {
        Object response = askTracker(message);
        if (response.getClass() != Boolean.class) {
            throw new SparkException("Error reply received from MapOutputTracker. Expecting true, got " + message.toString());
        }
    }

    /**
     * @return key = BlockManagerId(Shuffle数据存储Executor) value = [key = BlockId, value = 数据块大小]
     * */
    public abstract Multimap<BlockManagerId, Tuple2<BlockId, Long>> getMapSizesByExecutorId(int shuffleId, int startPartition, int endPartition);

    public abstract void unregisterShuffle(int shuffleId);

    public abstract void stop();

    // Serialize an array of map output locations into an efficient byte format so that we can send
    // it to reduce tasks. We do this by compressing the serialized bytes using GZIP. They will
    // generally be pretty compressible because many map outputs will be on the same hostname.
    public static Pair<byte[], Broadcast<byte[]>> serializeMapStatuses(MapStatus[] statuses, BroadcastManager broadcastManager,
                                                                       boolean isLocal, int minBroadcastSize) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(DIRECT);
        ObjectOutputStream objOut = new ObjectOutputStream(new GZIPOutputStream(out));
        synchronized (statuses) {
            objOut.writeObject(statuses);
        }
        objOut.close();

        byte[] arr = out.toByteArray();
        if (arr.length >= minBroadcastSize) {
            // Use broadcast instead.
            // Important arr(0) is the tag == DIRECT, ignore that while deserializing !
            Broadcast<byte[]> bcast = broadcastManager.newBroadcast(arr, isLocal);
            // toByteArray creates copy, so we can reuse out
            out.reset();
            out.write(BROADCAST);
            ObjectOutputStream oos = new ObjectOutputStream(new GZIPOutputStream(out));
            oos.writeObject(bcast);
            oos.close();
            byte[] outArr = out.toByteArray();
            LOGGER.info("Broadcast mapstatuses size = {}, actual size = {}", outArr.length, arr.length);
            return ImmutablePair.of(outArr, bcast);
        }

        return ImmutablePair.of(arr, null);
    }

    private static Object deserializeObject(byte[] arr, int off, int len) throws IOException, ClassNotFoundException {
        ObjectInputStream objIn = null;
        try {
            objIn = new ObjectInputStream(new GZIPInputStream(new ByteArrayInputStream(arr, off, len)));
            return objIn.readObject();
        } finally {
            if (objIn != null) {
                objIn.close();
            }
        }
    }

    public static MapStatus[] deserializeMapStatuses(byte[] bytes) {
        assert (bytes.length > 0);
        int type = bytes[0];
        try {
            switch (type) {
                case DIRECT:
                    return (MapStatus[]) deserializeObject(bytes, 1, bytes.length - 1);
                case BROADCAST:
                    Broadcast<byte[]> broadcast = (Broadcast<byte[]>) deserializeObject(bytes, 1, bytes.length -1);
                    byte[] broadcastValue = broadcast.value();
                    return (MapStatus[]) deserializeObject(broadcastValue, 1, broadcastValue.length -1);
                default:
                    throw new UnsupportedOperationException("Unsupported map status type : " + type);
            }
        } catch (Exception e) {
            return null;
        }
    }

    public static Multimap<BlockManagerId, Tuple2<BlockId, Long>> convertMapStatuses(int shuffleId,
                                                                                               int startPartition,
                                                                                               int endPartition,
                                                                                               MapStatus[] statuses) {
        assert (statuses != null);
        Multimap<BlockManagerId, Tuple2<BlockId, Long>> splitsByAddress = LinkedHashMultimap.create();
        for (int i = 0; i < statuses.length; ++i) {
            MapStatus status = statuses[i];
            if (status == null) {
                String errorMessage = String.format("Missing an output location for shuffle %d", shuffleId);
                LOGGER.error(errorMessage);
                throw new MetadataFetchFailedException(shuffleId, startPartition, errorMessage);
            }
            for (int part = startPartition; part <= endPartition; ++part) {
                ShuffleBlockId shuffleBlockId = new ShuffleBlockId(shuffleId, i, part);
                Tuple2<BlockId, Long> blockIdInfo = new Tuple2<>(shuffleBlockId, status.getSizeForBlock(part));
                splitsByAddress.put(status.location(), blockIdInfo);
            }
        }

        return splitsByAddress;
    }
}
