package com.sdu.spark.storage;

import com.google.common.collect.Maps;
import com.sdu.spark.SecurityManager;
import com.sdu.spark.SparkException;
import com.sdu.spark.network.utils.JavaUtils;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.utils.ChunkedByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.ConcurrentMap;

/**
 * todo: 文件读写安全验证
 *
 * @author hanhan.zhang
 * */
public class DiskStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(DiskStore.class);

    public SparkConf conf;
    public DiskBlockManager diskManager;
    public SecurityManager securityManager;

    private long minMemoryMapBytes;
    private ConcurrentMap<String, Long> blockSizes;

    public DiskStore(SparkConf conf, DiskBlockManager diskManager, SecurityManager securityManager) {
        this.conf = conf;
        this.diskManager = diskManager;
        this.securityManager = securityManager;

        this.minMemoryMapBytes = conf.getSizeAsBytes("spark.storage.memoryMapThreshold", "2m");
        this.blockSizes = Maps.newConcurrentMap();
    }

    public boolean contains(BlockId blockId) {
        try {
            return diskManager.containsBlock(blockId);
        } catch (IOException e) {
            throw  new SparkException(e);
        }

    }

    public void put(BlockId blockId, SpillDataToDisk blockWriter) throws IOException {
        if (contains(blockId)) {
            throw new IllegalStateException("Block $blockId is already present in the disk store");
        }
        LOGGER.info("Attempting to put block {}", blockId);
        long startTime = System.currentTimeMillis();
        File file = diskManager.getFile(blockId);
        CountingWritableChannel out = new CountingWritableChannel(openForWrite(file));
        boolean threwException = true;
        try {
            blockWriter.writeDisk(out);
            blockSizes.put(blockId.name(), out.getCount());
            threwException = false;
        } finally {
            try {
                out.close();
            } catch (IOException e){
                if (!threwException) {
                    threwException = true;
                    throw e;
                }
            } finally {
                if (threwException) {
                    remove(blockId);
                }
            }
        }

        long finishTime = System.currentTimeMillis();
        LOGGER.info("Block {} stored as {} file on disk in {} ms",
                    file.getName(),
                    file.length(),
                    finishTime - startTime);
    }

    public void putBytes(BlockId blockId, ChunkedByteBuffer bytes) {
        try {
            put(blockId, channel -> {
                try {
                    bytes.writeFully(channel);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new SparkException("dist store block " + blockId + " failure", e);
        }
    }

    public long getSize(BlockId blockId) {
        return blockSizes.get(blockId.name());
    }

    public BlockData getBytes(BlockId blockId) {
        try {
            File file = diskManager.getFile(blockId);
            long blockSize = getSize(blockId);

            // TODO: 安全验证
            FileChannel channel = new FileInputStream(file).getChannel();
            try {
                if (blockSize < minMemoryMapBytes) {
                    ByteBuffer buf = ByteBuffer.allocate((int) blockSize);
                    JavaUtils.readFully(channel, buf);
                    buf.flip();
                    return new BlockData.ByteBufferBlockData(new ChunkedByteBuffer(buf), true);
                } else {
                    ByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, file.length());
                    return new BlockData.ByteBufferBlockData(new ChunkedByteBuffer(buffer), true);
                }
            } finally {
                channel.close();
            }
        } catch (IOException e) {
            throw new SparkException("Got block " + blockId + " data failure", e);
        }

    }

    private WritableByteChannel openForWrite(File file) throws FileNotFoundException {
        // TODO: 安全认证
        return new FileOutputStream(file).getChannel();
    }

    public boolean remove(BlockId blockId)  {
        blockSizes.remove(blockId.name());
        File file = diskManager.getFile(blockId);
        if (file.exists()) {
            boolean ret = file.delete();
            if (!ret) {
                LOGGER.error("Error deleting {}", file.getPath());
            }
            return ret;
        }
        return false;
    }

    private class CountingWritableChannel implements WritableByteChannel {

        private long count = 0L;

        private WritableByteChannel slink;

        CountingWritableChannel(WritableByteChannel slink) {
            this.slink = slink;
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            int writeBytes = slink.write(src);
            if (writeBytes > 0) {
                // 注意: -1
                count += writeBytes;
            }
            return writeBytes;
        }

        @Override
        public boolean isOpen() {
            return slink.isOpen();
        }

        @Override
        public void close() throws IOException {
            slink.close();
        }

        public long getCount() {
            return count;
        }
    }

    public interface SpillDataToDisk {
        void writeDisk(WritableByteChannel channel);
    }
}
