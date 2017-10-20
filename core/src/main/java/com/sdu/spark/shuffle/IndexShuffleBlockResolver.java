package com.sdu.spark.shuffle;

import com.google.common.io.ByteStreams;
import com.sdu.spark.SparkEnv;
import com.sdu.spark.SparkException;
import com.sdu.spark.network.buffer.FileSegmentManagedBuffer;
import com.sdu.spark.network.buffer.ManagedBuffer;
import com.sdu.spark.network.netty.SparkTransportConf;
import com.sdu.spark.network.utils.TransportConf;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.storage.BlockId.*;
import com.sdu.spark.storage.BlockManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;

/**
 * {@link IndexShuffleBlockResolver}负责Shuffle File创建及Shuffle数据读取
 *
 *  1: shuffle数据有两种文件: shuffle数据文件、shuffle索引文件
 *
 * @author hanhan.zhang
 * */
public class IndexShuffleBlockResolver implements ShuffleBlockResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexShuffleBlockResolver.class);

    // No-op reduce ID used in interactions with disk store.
    // The disk store currently expects puts to relate to a (map, reduce) pair, but in the sort
    // shuffle outputs for several reduces are glommed into a single file.
    private static final int NOOP_REDUCE_ID = 0;

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

    private File getDataFile(int shuffleId, int mapId) {
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

    @Override
    public void stop() {

    }
}
