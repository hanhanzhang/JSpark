package com.sdu.spark.serializer;

import com.sdu.spark.io.CompressionCodec;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.BlockId.*;
import com.sdu.spark.utils.ChunkedByteBuffer;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * {@link SerializerManager}职责
 *
 * 1: 负责Input/Output Stream数据流压缩
 *
 *   1' {@link #wrapStream(BlockId, InputStream)}输入数据流解压缩
 *
 *   2' {@link #wrapStream(BlockId, OutputStream)}输出数据流压缩
 *
 * 2:
 *
 * @author hanhan.zhang
 * */
public class SerializerManager {

    // Whether to compress broadcast variables that are stored
    private boolean compressBroadcast;
    // Whether to compress shuffle output that are stored
    private boolean compressShuffle;
    // Whether to compress RDD partitions that are stored serialized
    private boolean compressRdds;
    // Whether to compress shuffle output temporarily spilled to disk
    private boolean compressShuffleSpill;

    /**
     * The compression codec to use. Note that the "lazy" val is necessary because we want to delay
     * the initialization of the compression codec until it is first used. The reason is that a Spark
     * program could be using a user-defined codec in a third party jar, which is loaded in
     * Executor.updateDependencies. When the BlockManager is initialized, user level jars hasn't been
     * loaded yet.
     * */
    private CompressionCodec compressionCodec;

    private Serializer defaultSerializer;
    private SparkConf conf;
    private byte[] encryptionKey;

    public SerializerManager(Serializer defaultSerializer,
                             SparkConf conf) {
        this(defaultSerializer, conf, null);
    }

    public SerializerManager(Serializer defaultSerializer,
                             SparkConf conf,
                             byte[] encryptionKey) {
        this.defaultSerializer = defaultSerializer;
        this.conf = conf;
        this.encryptionKey = encryptionKey;

        this.compressBroadcast = conf.getBoolean("spark.broadcast.compress", true);
        this.compressShuffle = conf.getBoolean("spark.shuffle.compress", true);
        this.compressRdds = conf.getBoolean("spark.rdd.compress", false);
        this.compressShuffleSpill = conf.getBoolean("spark.shuffle.spill.compress", true);
    }

    public InputStream wrapStream(BlockId blockId, InputStream s) {
        return wrapForCompression(blockId, s);
    }

    /**
     * Wrap an input stream for compression if block compression is enabled for its block type
     * */
    private InputStream wrapForCompression(BlockId blockId, InputStream s) {
        if (shouldCompress(blockId)) {
            return compressionCodec().compressedInputStream(s);
        }
        return s;
    }

    public OutputStream wrapStream(BlockId blockId, OutputStream s) {
       return wrapForCompression(blockId, s);
    }

    /**
     * Wrap an output stream for compression if block compression is enabled for its block type
     * */
    private OutputStream wrapForCompression(BlockId blockId, OutputStream s) {
        if (shouldCompress(blockId)) {
            return compressionCodec().compressedOutputStream(s);
        }
        return s;
    }

    public ChunkedByteBuffer dataSerializeWithExplicitClassTag(BlockId blockId, Iterator<?> values) {
        throw new UnsupportedOperationException("");
    }

    public <T> Iterator<T> dataDeserializeStream(BlockId blockId, InputStream inputStream) {
        throw new UnsupportedOperationException("");
    }

    private CompressionCodec compressionCodec() {
        if (compressionCodec == null) {
            synchronized (this) {
                if (compressionCodec == null) {
                    compressionCodec = CompressionCodec.createCodec(conf);
                }
            }
        }
        return compressionCodec;
    }

    private boolean shouldCompress(BlockId blockId) {
        if (blockId instanceof ShuffleBlockId) {
            return compressShuffle;
        }
        if (blockId instanceof BroadcastBlockId) {
            return compressBroadcast;
        }
        if (blockId instanceof RDDBlockId) {
            return compressRdds;
        }
        if (blockId instanceof TempLocalBlockId) {
            return compressShuffleSpill;
        }
        if (blockId instanceof TempShuffleBlockId) {
            return compressShuffle;
        }
        return false;
    }
}
