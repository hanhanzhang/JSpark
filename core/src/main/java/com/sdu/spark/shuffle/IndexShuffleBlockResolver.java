package com.sdu.spark.shuffle;

import com.google.common.io.ByteStreams;
import com.sdu.spark.SparkEnv;
import com.sdu.spark.SparkException;
import com.sdu.spark.io.NioBufferedFileInputStream;
import com.sdu.spark.network.buffer.FileSegmentManagedBuffer;
import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.network.netty.SparkTransportConf;
import com.sdu.spark.network.utils.TransportConf;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.storage.BlockId.*;
import com.sdu.spark.storage.BlockManager;
import com.sdu.spark.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;

/**
 * {@link IndexShuffleBlockResolver}负责Shuffle File创建及Shuffle数据读取
 *
 *  1: shuffle数据有两种文件: shuffle数据文件、shuffle索引文件(记录每个Block在数据文件中偏移量)
 *
 * @author hanhan.zhang
 * */
public class IndexShuffleBlockResolver implements ShuffleBlockResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexShuffleBlockResolver.class);

    // No-op reduce ID used in interactions with disk store.
    // The disk store currently expects puts to relate to a (map, reduce) pair, but in the sort
    // shuffle outputs for several reduces are glommed into a single file.
    public static final int NOOP_REDUCE_ID = 0;

    private SparkConf conf;
    private BlockManager blockManager;

    private TransportConf transportConf;

    public IndexShuffleBlockResolver(SparkConf conf) {
        this(conf, null);
    }

    public IndexShuffleBlockResolver(SparkConf conf,
                                     BlockManager blockManager) {
        this.conf = conf;
        this.blockManager = blockManager;
        this.transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle");
    }

    public File getDataFile(int shuffleId, int mapId) {
        if (blockManager == null) {
            synchronized (IndexShuffleBlockResolver.class) {
                if (blockManager == null) {
                    blockManager = SparkEnv.env.blockManager;
                }
            }
        }
        return blockManager.diskBlockManager.getFile(new ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID));
    }

    private File getIndexFile(int shuffleId, int mapId) {
        if (blockManager == null) {
            synchronized (IndexShuffleBlockResolver.class) {
                if (blockManager == null) {
                    blockManager = SparkEnv.env.blockManager;
                }
            }
        }
        return blockManager.diskBlockManager.getFile(new ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID));
    }

    /**
     * Check whether the given index and data files match each other.
     * If so, return the partition lengths in the data file. Otherwise return null.
     * */
    private long[] checkIndexAndDataFile(File index, File data, int blocks) {
        // index记录offset = 0, 故blocks需加1
        if (index.length() != (blocks + 1) * 8) {
            return null;
        }
        long[] lengths = new long[blocks];

        // 读取每个block data file长度(即偏移量)
        try {
            DataInputStream in = new DataInputStream(new NioBufferedFileInputStream(index));
            long offset = in.readLong();
            if (offset != 0) {
                return null;
            }
            int i = 0;
            long sum = 0;
            while (i < blocks) {
                long off = in.readLong();
                long length = off - offset;
                sum += length;
                lengths[i] = length;
                offset = off;
                i += 1;
            }
            in.close();

            if (sum == data.length()) {
                return lengths;
            } else {
                return null;
            }
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public ManagedBuffer getBlockData(ShuffleBlockId blockId) {
        File indexFile = getIndexFile(blockId.shuffleId, blockId.mapId);
        DataInputStream in = null;
        try {
            in = new DataInputStream(Files.newInputStream(indexFile.toPath()));
            /**@see ShuffleWriter*/
            ByteStreams.skipFully(in, blockId.reduceId * 8);
            long offset = in.readLong();
            long nextOffset = in.readLong();
            return new FileSegmentManagedBuffer(
                    transportConf,
                    getDataFile(blockId.shuffleId, blockId.mapId),
                    offset,
                    nextOffset - offset);
        } catch (IOException e) {
            LOGGER.error("Got shuffle block {} data failure", blockId, e);
            throw new SparkException(String.format("Got shuffle block %s data failure", blockId),e);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                LOGGER.debug("close block {} shuffle file failure", blockId, e);
            }

        }
    }

    public void removeDataByMap(int shuffleId, int mapId) {
        File file = getDataFile(shuffleId, mapId);
        if (file.exists()) {
            if (!file.delete()) {
                LOGGER.warn("Error delete shuffle data file {} failure", file.getPath());
            }
        }

        file = getIndexFile(shuffleId, mapId);
        if (file.exists()) {
            if (!file.delete()) {
                LOGGER.warn("Error delete shuffle index file {} failure", file.getPath());
            }
        }
    }

    /**
     * Write an index file with the offsets of each block, plus a final offset at the end for the
     * end of the output file. This will be used by getBlockData to figure out where each block
     * begins and ends.
     *
     * It will commit the data and index file as an atomic operation, use the existing ones, or
     * replace them with new ones.
     *
     * Note: the `lengths` will be updated to match the existing index file if use the existing ones.
     * */
    public void writeIndexFileAndCommit(int shuffleId, int mapId, long[] lengths, File dataTmp) {
        File indexFile = getIndexFile(shuffleId, mapId);
        File indexTmp = Utils.tempFileWith(indexFile);
        try {
            DataOutputStream out = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(indexTmp.toPath())));
            // 记录分区Shuffle数据文件偏移量
            long offset = 0L;
            out.writeLong(offset);
            for (int i = 0; i < lengths.length; ++i) {
                offset += lengths[i];
                out.writeLong(offset);
            }
            out.close();

            File dataFile = getDataFile(shuffleId, mapId);
            // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
            // the following check and rename are atomic.
            synchronized (this) {
                long[] existingLengths = checkIndexAndDataFile(indexTmp, dataFile, lengths.length);
                if (existingLengths != null) {
                    System.arraycopy(existingLengths, 0, lengths, 0, lengths.length);
                    if (dataTmp != null && dataTmp.exists()) {
                        dataTmp.delete();
                    }
                    indexFile.delete();
                } else {
                    // This is the first successful attempt in writing the map outputs for this task,
                    // so override any existing index and data files with the ones we wrote.
                    if (indexFile.exists()) {
                        indexFile.delete();
                    }
                    if (dataFile.exists()) {
                        dataFile.delete();
                    }
                    if (!indexTmp.renameTo(indexFile)) {
                        throw new IOException("fail to rename file " + indexTmp + " to " + indexFile);
                    }
                    if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
                        throw new IOException("fail to rename file " + dataTmp + " to " + dataFile);
                    }
                }
            }

        } catch (IOException e) {
            LOGGER.error("write shuffle {} index file {} failure", shuffleId, indexTmp.getPath(), e);
        } finally {
            if (indexTmp.exists() && !indexTmp.delete()) {
                LOGGER.error("Failed to delete temporary index file at {}", indexTmp.getAbsolutePath());
            }
        }
    }

    @Override
    public void stop() {

    }
}
