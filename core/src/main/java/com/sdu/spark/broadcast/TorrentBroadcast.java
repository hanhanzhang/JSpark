package com.sdu.spark.broadcast;

import com.sdu.spark.SparkEnv;
import com.sdu.spark.SparkException;
import com.sdu.spark.TaskContext;
import com.sdu.spark.io.CompressionCodec;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.serializer.DeserializationStream;
import com.sdu.spark.serializer.SerializationStream;
import com.sdu.spark.serializer.Serializer;
import com.sdu.spark.serializer.SerializerInstance;
import com.sdu.spark.storage.BlockData;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.BlockManager;
import com.sdu.spark.storage.BlockResult;
import com.sdu.spark.utils.ChunkedByteBuffer;
import com.sdu.spark.utils.io.ChunkedByteBufferOutputStream;
import org.apache.commons.collections.map.ReferenceMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.Adler32;

import static com.sdu.spark.storage.StorageLevel.MEMORY_AND_DISK;
import static com.sdu.spark.storage.StorageLevel.MEMORY_AND_DISK_SER;
import static java.lang.String.format;

/**
 * 基于 "BitTorrent" 协议
 *
 *
 * @author hanhan.zhang
 * */
public class TorrentBroadcast<T> extends Broadcast<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TorrentBroadcast.class);

    private transient T value;
    private transient CompressionCodec compressionCodec;
    /** Broadcast Data split to Block */
    private transient long blockSize;

    /** 需序列化传输, Executor 端需知切分的Block数目 */
    private int numBlocks;
    private BlockId.BroadcastBlockId broadcastId;
    private boolean checksumEnabled = false;
    private int[] checksums;

    public TorrentBroadcast(long id, T obj) {
        super(id);
        this.numBlocks = writeBlocks(obj);
        this.broadcastId = new BlockId.BroadcastBlockId(id, "");
        setConf(SparkEnv.env.conf);
    }

    private void setConf(SparkConf conf) {
        if (conf.getBoolean("spark.broadcast.compress", true)) {
            compressionCodec = CompressionCodec.createCodec(conf);
        }
        blockSize = conf.getSizeAsKb("spark.broadcast.blockSize", "4M") * 1024;
        checksumEnabled = conf.getBoolean("spark.broadcast.checksum", true);
    }

    @Override
    protected T getValue() {
        if (value == null) {
            value = readBroadcastBlock();
        }
        return value;
    }

    @Override
    protected void doDestroy(boolean blocking) {
        unpersist(id, true, blocking);
    }

    @Override
    protected void doUnpersist(boolean blocking) {
        unpersist(id, false, blocking);
    }

    /** Used by the JVM when serializing this object. */
    private void writeObject(ObjectOutputStream out) throws IOException{
        assertValid();
        out.defaultWriteObject();
    }


    @SuppressWarnings("unchecked")
    private synchronized T readBroadcastBlock() {
        ReferenceMap broadcastCache = SparkEnv.env.broadcastManager.cacheValues;
        Object value = broadcastCache.get(broadcastId);
        if (value != null) {
            return (T) value;
        }

        setConf(SparkEnv.env.conf);
        BlockManager blockManager = SparkEnv.env.blockManager;
        BlockResult blockResult = blockManager.getLocalValues(broadcastId);
        if (blockResult != null) {
            if (blockResult.getData().hasNext()) {
                T broadcastData = (T) blockResult.getData().next();
                releaseLock(broadcastId);
                if (broadcastData != null) {
                    broadcastCache.put(broadcastId, broadcastData);
                }
                return broadcastData;
            }
            throw new SparkException(format("Failed to get locally stored broadcast data: %s", broadcastId));
        }

        // 逐步读取数据块
        LOGGER.info("Starting reading broadcast variable ", id);

        long startTimeMs = System.currentTimeMillis();
        BlockData[] blocks = readBlocks();
        LOGGER.info("Reading broadcast variable {} took {}ms", id, System.currentTimeMillis() - startTimeMs);

        try {
            List<InputStream> inputStreams = Arrays.stream(blocks).map(BlockData::toInputStream).collect(Collectors.toList());
            T obj = TorrentBroadcast.unBlockifyObject(inputStreams, SparkEnv.env.serializer, compressionCodec);
            if (!blockManager.putSingle(broadcastId, obj, MEMORY_AND_DISK, false)) {
                throw new SparkException(format("Failed to store %s in BlockManager", broadcastId));
            }

            if (obj != null) {
                broadcastCache.put(broadcastId, obj);
            }

            return obj;
        } finally {
            for (BlockData blockData : blocks) {
                blockData.dispose();
            }
        }
    }


    /** Fetch torrent blocks from the driver and/or other executors. */
    private BlockData[] readBlocks() {
        BlockData[] blocks = new BlockData[numBlocks];
        BlockManager bm = SparkEnv.env.blockManager;

        List<Integer> pieces = new ArrayList<>();
        for (int i = 0; i < numBlocks; ++i) {
            pieces.add(i);
        }
        Collections.shuffle(pieces);

        //
        for (int piece : pieces) {
            BlockId.BroadcastBlockId pieceId = new BlockId.BroadcastBlockId(id, "piece" + piece);
            LOGGER.debug("Reading piece {} of {}", pieceId, broadcastId);
            BlockData blockData = bm.getLocalBytes(pieceId);
            if (blockData != null) {
                blocks[piece] = blockData;
                releaseLock(pieceId);
            } else {
                ChunkedByteBuffer buffer = bm.getRemoteBytes(pieceId);
                if (buffer != null) {
                    if (checksumEnabled) {
                        int sum = calcChecksum(buffer.chunks[0]);
                        if (sum != checksums[piece]) {
                            throw new SparkException(format("corrupt remote block %s of %s: %s != %s", pieceId, broadcastId, sum, checksums[piece]));
                        }
                    }

                    // 存放在本地, 并向Driver BlockManager汇报, BitTorrent协议
                    if (!bm.putBytes(pieceId, buffer, MEMORY_AND_DISK_SER, true)) {
                        throw new SparkException(format("Failed to store %s of %s in local BlockManager", pieceId, broadcastId));
                    }
                    blocks[piece] =  new BlockData.ByteBufferBlockData(buffer, true);
                } else {
                    throw new SparkException(format("Failed to get %s of %s", pieceId, broadcastId));
                }
            }
        }

        return blocks;
    }

    /**
     * If running in a task, register the given block's locks for release upon task completion.
     * Otherwise, if not running in a task then immediately release the lock.
     */
    private void releaseLock(BlockId blockId)  {
        BlockManager blockManager = SparkEnv.env.blockManager;
        TaskContext context = TaskContext.get();
        if (context != null) {
            context.addTaskCompletionListener(taskContext -> blockManager.releaseLock(blockId, -1));
        } else {
            blockManager.releaseLock(blockId, -1);
        }
    }

    /**
     * Divide the object into multiple blocks and put those blocks in the block manager.
     *
     * @param value the object to divide
     * @return number of blocks this broadcast variable is divided into
     * */
    private int writeBlocks(T value) {
        BlockManager blockManager = SparkEnv.env.blockManager;
        if (!blockManager.putSingle(broadcastId, value, MEMORY_AND_DISK, true)) {
            throw new SparkException(format("Failed to store %s in BlockManager", broadcastId));
        }

        ByteBuffer[] dataBlocks= blockifyObject(value, (int) blockSize, SparkEnv.env.serializer, compressionCodec);

        if (checksumEnabled) {
            checksums = new int[dataBlocks.length];
        }

        for (int i = 0; i < dataBlocks.length; ++i) {
            if (checksumEnabled) {
                checksums[i] = calcChecksum(dataBlocks[i]);
            }

            BlockId.BroadcastBlockId pieceId = new BlockId.BroadcastBlockId(id, "piece" + i);
            ChunkedByteBuffer bytes = new ChunkedByteBuffer(dataBlocks[i].duplicate());
            if (!blockManager.putBytes(pieceId, bytes, MEMORY_AND_DISK_SER, true)) {
                throw new SparkException(format("Failed to store %s of %s in local BlockManager", pieceId, broadcastId));
            }
        }

        return dataBlocks.length;
    }

    private int calcChecksum(ByteBuffer block) {
        Adler32 adler = new Adler32();
        if (block.hasArray()) {
            adler.update(block.array(), block.arrayOffset() + block.position(), block.limit()
                    - block.position());
        } else {
            byte[] bytes = new byte[block.remaining()];
            block.duplicate().get(bytes);
            adler.update(bytes);
        }
        return (int) adler.getValue();
    }

    private static <T> ByteBuffer[] blockifyObject(T obj, int blockSize, Serializer serializer, CompressionCodec compressionCodec) {
        try {
            ChunkedByteBufferOutputStream  cbbos = new ChunkedByteBufferOutputStream(blockSize, ByteBuffer::allocate);
            OutputStream out = compressionCodec != null ? compressionCodec.compressedOutputStream(cbbos) : cbbos;
            SerializerInstance ser = serializer.newInstance();
            SerializationStream serOut = ser.serializeStream(out);
            serOut.writeObject(obj);
            serOut.close();
            return cbbos.toChunkedByteBuffer().chunks;
        } catch (IOException e) {
            throw new SparkException("Broadcast data split data block failure", e);
        }
    }

    private static <T> T unBlockifyObject(List<InputStream> blockData, Serializer serializer, CompressionCodec compressionCodec) {
        if (blockData == null || blockData.size() == 0) {
            throw new IllegalArgumentException("Cannot unblockify an empty array of blocks");
        }

        try {
            SequenceInputStream is = new SequenceInputStream(new Vector<>(blockData).elements());
            InputStream in = compressionCodec != null ? compressionCodec.compressedInputStream(is) : is;
            SerializerInstance ser = serializer.newInstance();
            DeserializationStream serIn = ser.deserializeStream(in);
            T obj = serIn.readObject();
            serIn.close();
            return obj;
        } catch (IOException e) {
            throw new SparkException("Broadcast data convert block to data failure", e);        }
    }

    private static void unpersist(long id, boolean removeFromDriver, boolean blocking) {
        LOGGER.debug("Unpersisting TorrentBroadcast: {}", id);
        SparkEnv.env.blockManager.master.removeBroadcast(id, removeFromDriver, blocking);
    }
}
