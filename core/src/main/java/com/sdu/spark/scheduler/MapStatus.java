package com.sdu.spark.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.sdu.spark.SparkEnv;
import com.sdu.spark.storage.BlockManagerId;
import org.roaringbitmap.RoaringBitmap;

import java.io.*;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * {@link MapStatus} 记录MapTask运行状态信息
 *
 *  1: {@link #location()} 标识MapTask运行所属机器信息
 *
 *  2: {@link #getSizeForBlock(int)} 获取分区数据
 *
 * @author hanhan.zhang
 * */
public interface MapStatus extends Serializable {

    double LOG_BASE = 1.1;

    BlockManagerId location();

    long getSizeForBlock(int reduceId);

    class HighlyCompressedMapStatus implements MapStatus, Externalizable {

        private BlockManagerId loc;
        private int numNonEmptyBlocks;
        private RoaringBitmap emptyBlocks;
        private long avgSize;
        private Map<Integer, Byte> hugeBlockSizes;

        public HighlyCompressedMapStatus() {
            this(null, -1, null, -1, null);
        }

        /**
         * {@link MapStatus} implementation that stores the accurate size of huge blocks, which are larger
         * than spark.shuffle.accurateBlockThreshold. It stores the average size of other non-empty blocks,
         * plus a bitmap for tracking which blocks are empty.
         *
         * @param loc location where the task is being executed
         * @param numNonEmptyBlocks the number of non-empty blocks
         * @param emptyBlocks a bitmap tracking which blocks are empty
         * @param avgSize average size of the non-empty and non-huge blocks
         * @param hugeBlockSizes sizes of huge blocks by their reduceId.
         * */
        public HighlyCompressedMapStatus(BlockManagerId loc,
                                         int numNonEmptyBlocks,
                                         RoaringBitmap emptyBlocks,
                                         long avgSize,
                                         Map<Integer, Byte> hugeBlockSizes) {
            checkArgument(loc == null || avgSize > 0 || hugeBlockSizes.size() > 0 || numNonEmptyBlocks == 0,
                          "Average size can only be zero for map stages that produced no combiner");

            this.loc = loc;
            this.numNonEmptyBlocks = numNonEmptyBlocks;
            this.emptyBlocks = emptyBlocks;
            this.avgSize = avgSize;
            this.hugeBlockSizes = hugeBlockSizes;
        }

        @Override
        public BlockManagerId location() {
            return loc;
        }

        @Override
        public long getSizeForBlock(int reduceId) {
            assert hugeBlockSizes != null;
            if (emptyBlocks.contains(reduceId)) {
                return 0L;
            } else {
                Byte blockSize = hugeBlockSizes.get(reduceId);
                return blockSize == null ? avgSize : decompressSize(blockSize);
            }
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            loc.writeExternal(out);
            emptyBlocks.writeExternal(out);
            out.writeLong(avgSize);
            out.writeInt(hugeBlockSizes.size());
            for (Map.Entry<Integer, Byte> entry : hugeBlockSizes.entrySet()) {
                out.writeInt(entry.getKey());
                out.writeByte(entry.getValue());
            }
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            loc = BlockManagerId.apply(in);
            emptyBlocks = new RoaringBitmap();
            emptyBlocks.readExternal(in);
            avgSize = in.readLong();
            int count = in.readInt();
            hugeBlockSizes = Maps.newLinkedHashMapWithExpectedSize(count);
            for (int i = 0; i < count; ++i) {
                int block = in.readInt();
                byte size = in.readByte();
                hugeBlockSizes.put(block, size);
            }
        }

        public static HighlyCompressedMapStatus apply(BlockManagerId loc, long[] uncompressedSizes) {
            int i = 0;
            int numNonEmptyBlocks = 0;
            long totalSize = 0L;

            RoaringBitmap emptyBlocks = new RoaringBitmap();
            int totalNumBlocks = uncompressedSizes.length;

            long threshold = SparkEnv.env.conf.getLong("spark.shuffle.accurateBlockThreshold", 100 * 1024 * 1024);
            Map<Integer, Byte> hugeBlockSizes = Maps.newLinkedHashMap();

            while (i < totalNumBlocks) {
                long size = uncompressedSizes[i];
                if (size > 0) {
                    numNonEmptyBlocks += 1;
                    // Huge blocks are not included in the calculation for average size, thus size for smaller
                    // blocks is more accurate.
                    if (size < threshold) {
                        totalSize += size;
                    } else {
                        hugeBlockSizes.put(i, compressSize(size));
                    }
                } else {
                    emptyBlocks.add(i);
                }
                ++i;
            }

            long avgSize = numNonEmptyBlocks > 0 ? totalSize / numNonEmptyBlocks : 0;
            emptyBlocks.trim();
            emptyBlocks.runOptimize();

            return new HighlyCompressedMapStatus(loc,
                                                 numNonEmptyBlocks,
                                                 emptyBlocks,
                                                 avgSize,
                                                 hugeBlockSizes);
        }
    }

    class CompressedMapStatus implements MapStatus, Externalizable {

        private BlockManagerId loc;
        private byte[] compressedSizes;

        public CompressedMapStatus(BlockManagerId loc, long[] uncompressedSizes) {
            byte[] compressedSizes = new byte[uncompressedSizes.length];
            for (int i = 0; i < uncompressedSizes.length; ++i) {
                compressedSizes[i] = compressSize(uncompressedSizes[i]);
            }
            this.loc = loc;
            this.compressedSizes = compressedSizes;
        }

        /**
         * @param loc location where the task is being executed.
         * @param compressedSizes size of the blocks, indexed by reduce partition id.
         * */
        public CompressedMapStatus(BlockManagerId loc,
                                   byte[] compressedSizes) {
            this.loc = loc;
            this.compressedSizes = compressedSizes;
        }

        @Override
        public BlockManagerId location() {
            return loc;
        }

        @Override
        public long getSizeForBlock(int reduceId) {
            return decompressSize(compressSize(reduceId));
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            loc.writeExternal(out);
            out.writeInt(compressedSizes.length);
            out.write(compressedSizes);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            loc = BlockManagerId.apply(in);
            long len = in.readLong();
            compressedSizes = new byte[(int) len];
            in.readFully(compressedSizes);
        }
    }

    /**
     * Compress a size in bytes to 8 bits for efficient reporting of map combiner sizes.
     * We do this by encoding the log base 1.1 of the size as an integer, which can support
     * sizes up to 35 GB with at most 10% error.
     */
    static byte compressSize(long size) {
        if (size == 0) {
            return 0;
        } else if (size <= 1L) {
            return 1;
        } else {
            return (byte) Math.min(255, Math.ceil(Math.log(size) / Math.log(LOG_BASE)));
        }
    }

    /**
     * Decompress an 8-bit encoded block size, using the reverse operation of compressSize.
     */
    static long decompressSize(byte compressedSize) {
        if (compressedSize == 0) {
            return 0;
        } else {
            return (long) Math.pow(LOG_BASE, compressedSize & 0xFF);
        }
    }

    static MapStatus apply(BlockManagerId loc, long[] uncompressedSizes) {
        if (uncompressedSizes.length > 2000) {
            return HighlyCompressedMapStatus.apply(loc, uncompressedSizes);
        } else {
            return new CompressedMapStatus(loc, uncompressedSizes);
        }
    }
}
