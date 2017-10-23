package com.sdu.spark.utils.colleciton;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.sdu.spark.Aggregator.CreateAggregatorInitialValue;
import com.sdu.spark.Aggregator.CreateAggregatorMergeValue;
import com.sdu.spark.Aggregator.CreateAggregatorOutput;
import com.sdu.spark.SparkEnv;
import com.sdu.spark.SparkException;
import com.sdu.spark.TaskContext;
import com.sdu.spark.memory.MemoryConsumer;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.serializer.DeserializationStream;
import com.sdu.spark.serializer.Serializer;
import com.sdu.spark.serializer.SerializerInstance;
import com.sdu.spark.serializer.SerializerManager;
import com.sdu.spark.storage.*;
import com.sdu.spark.utils.CompletionIterator;
import com.sdu.spark.utils.Utils;
import com.sdu.spark.utils.scala.Product2;
import com.sdu.spark.utils.scala.Tuple2;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * {@link ExternalAppendOnlyMap}类似Hadoop MapReduce中shuffle-merge-combine-sort过程
 *
 * 1: {@link #currentMap}维护内存中Key-Value数据信息(每次插入/更新操作都会触发估算内存占用量, 超过阈值Spill内存数据到Disk)
 *
 * 2: {@link #spilledMaps}维护已Spill到Disk中数据信息(记录每个Batch在文件中偏移量, 便于Batch数据读取)
 *
 *
 * TODO:
 *
 * 1: {@link ExternalIterator}实现
 *
 *
 * @author hanhan.zhang
 * */
public class ExternalAppendOnlyMap<K, V, C> extends Spillable<AppendOnlyMap<K, C>> implements Serializable, Iterable<Tuple2<K, C>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExternalAppendOnlyMap.class);

    private CreateAggregatorInitialValue<V, C> initial;
    private CreateAggregatorMergeValue<V, C> merge;
    private CreateAggregatorOutput<C> output;

    private Serializer serializer;
    private BlockManager blockManager;
    private TaskContext context;
    private SerializerManager serializerManager;
    private SerializerInstance ser;

    private volatile SizeTrackingAppendOnlyMap<K, C> currentMap;
    private List<DiskMapIterator> spilledMaps;
    private SparkConf sparkConf;
    private DiskBlockManager diskBlockManager;

    private long serializerBatchSize;
    private int fileBufferSize;
    private long diskBytesSpilled = 0L;
    private long peakMemoryUsedBytes = 0L;

    private Comparator<K> keyComparator;
    private SpillableIterator readingIterator;

    public ExternalAppendOnlyMap(CreateAggregatorInitialValue<V, C> initial,
                                 CreateAggregatorMergeValue<V, C> merge,
                                 CreateAggregatorOutput<C> output) {
        super(TaskContext.get().taskMemoryManager());

        this.initial = initial;
        this.merge = merge;
        this.output = output;

        this.serializer = SparkEnv.env.closureSerializer;
        this.blockManager = SparkEnv.env.blockManager;
        this.context = TaskContext.get();
        this.serializerManager = SparkEnv.env.serializerManager;

        this.currentMap = new SizeTrackingAppendOnlyMap<>();
        this.spilledMaps = Lists.newArrayList();
        this.sparkConf = SparkEnv.env.conf;
        this.diskBlockManager = blockManager.diskBlockManager;
        this.ser = serializer.newInstance();

        this.serializerBatchSize = sparkConf.getLong("spark.shuffle.spill.batchSize", 10000);
        this.fileBufferSize = NumberUtils.toInt(sparkConf.getSizeAsKb("spark.shuffle.file.buffer", "32k")) * 1024;
        this.keyComparator = new HashComparator();
    }

    public long diskBytesSpilled() {
        return diskBytesSpilled;
    }

    public long peakMemoryUsedBytes() {
        return peakMemoryUsedBytes;
    }

    /**
     * Number of files this map has spilled so far.
     * */
    public int numSpill() {
        return spilledMaps.size();
    }

    /**
     * Force to spilling the current in-memory collection to disk to release memory,
     * It will be called by TaskMemoryManager when there is not enough memory for the task.
     * */
    @Override
    public boolean forceSpill() {
        if (readingIterator != null) {
            boolean isSpilled = readingIterator.spill();
            if (isSpilled) {
                currentMap = null;
            }
            return true;
        } else if (currentMap.size() > 0) {
            spill(currentMap);
            currentMap = new SizeTrackingAppendOnlyMap<>();
            return true;
        }
        return false;
    }

    @Override
    public void spill(AppendOnlyMap<K, C> collection) {
        Iterator<Tuple2<K, C>> inMemoryIterator = currentMap.destructiveSortedIterator(keyComparator);
        DiskMapIterator diskMapIterator = spillMemoryIteratorToDisk(inMemoryIterator);
        spilledMaps.add(diskMapIterator);
    }

    /**
     * 将内存中数据Spill到Disk
     * */
    private DiskMapIterator spillMemoryIteratorToDisk(Iterator<Tuple2<K, C>> inMemoryIterator) {
        try {
            Tuple2<BlockId, File> tuple2 = diskBlockManager.createTempLocalBlock();
            DiskBlockObjectWriter writer = blockManager.getDiskWriter(tuple2._1(),
                                                                      tuple2._2(),
                                                                      ser,
                                                                      fileBufferSize);
            long objectsWritten = 0L;
            // List of batch sizes (bytes) in the order they are written to disk
            List<Long> batchSizes = new ArrayList<>();

            boolean success = false;
            try {
                while (inMemoryIterator.hasNext()) {
                    Tuple2<K, C> kv = inMemoryIterator.next();
                    writer.write(kv._1(), kv._2());
                    objectsWritten += 1;
                    if (objectsWritten == serializerBatchSize) {
                        FileSegment segment = flush(writer);
                        batchSizes.add(segment.length);
                        objectsWritten = 0;
                    }
                }
                if (objectsWritten > 0) {
                    FileSegment segment = flush(writer);
                    batchSizes.add(segment.length);
                    writer.close();
                } else {
                    writer.revertPartialWritesAndClose();
                }
                success = true;

                return new DiskMapIterator(tuple2._2(), tuple2._1(), batchSizes);
            } catch (IOException e) {
                LOGGER.error("spill data from memory to disk failure", e);
                throw new SparkException("spill data from memory to disk failure", e);
            } finally {
                if (!success) {
                    // This code path only happens if an exception was thrown above before we set success;
                    // close our stuff and let the exception be thrown further
                    writer.revertPartialWritesAndClose();
                    if (tuple2._2().exists()) {
                        if (!tuple2._2().delete()) {
                            LOGGER.error("Error deleting ${file}");
                        }
                    }
                }
            }
        } catch (IOException e) {
            LOGGER.error("create temp block file failure", e);
            throw new SparkException("create temp block file failure", e);
        }

    }

    private FileSegment flush(DiskBlockObjectWriter writer) {
        FileSegment segment = writer.commitAndGet();
        diskBytesSpilled += segment.length;
        return segment;
    }

    /**
     * Insert the given key and value into the map.
     * */
    public void insert(K key, V value) {
        insertAll(Iterators.singletonIterator(new Tuple2<>(key, value)));
    }

    public void insertAll(Iterator<? extends Product2<K, V>> entries) {
        if (currentMap == null) {
            throw new IllegalStateException("Can't insert new elements into a map after calling iterator");
        }
        // Shuffle添加数据时, 若是达到Spill Disk阈值则将数据Spill到Disk
        Product2<K, V> curEntry = null;
        while (entries.hasNext()) {
            curEntry = entries.next();
            long estimatedSize = currentMap.estimateSize();
            if (estimatedSize > peakMemoryUsedBytes) {
                peakMemoryUsedBytes = estimatedSize;
            }
            if (maybeSpill(currentMap, estimatedSize)) {
                // currentMap中数据Spill到Disk中
                currentMap = new SizeTrackingAppendOnlyMap<>();
            }
            Updater updater = new Updater(curEntry);
            currentMap.changeValue(curEntry._1(), updater);
            addElementsRead();
        }
    }

    /**
     * Returns a destructive iterator for iterating over the entries of this map.
     * If this iterator is forced spill to disk to release memory when there is not enough memory,
     * it returns pairs from an on-disk map.
     */
    public Iterator<Tuple2<K, C>> destructiveIterator(Iterator<Tuple2<K, C>> inMemoryIterator) {
        readingIterator = new SpillableIterator(inMemoryIterator);
        return readingIterator;
    }

    private void freeCurrentMap(){
        if (currentMap != null) {
            currentMap = null; // So that the memory can be garbage-collected
            releaseMemory();
        }
    }

    /**
     * Return a destructive iterator that merges the in-memory map with the spilled maps.
     * If no spill has occurred, simply return the in-memory map's iterator.
     * */
    @Override
    public Iterator<Tuple2<K, C>> iterator() {
        if (currentMap == null) {
            throw new IllegalStateException("ExternalAppendOnlyMap.iterator is destructive and should only be called once.");
        }
        if (spilledMaps.isEmpty()) {
            return CompletionIterator.apply(destructiveIterator(currentMap.iterator()),
                                            this::freeCurrentMap);
        } else {
            return new ExternalIterator();
        }
    }

    /**
     * An iterator that sort-merges (K, C) pairs from the in-memory map and the spilled maps
     * */
    private class ExternalIterator implements Iterator<Tuple2<K, C>> {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Tuple2<K, C> next() {
            return null;
        }
    }

    private class DiskMapIterator implements Iterator<Tuple2<K, C>> {

        /**
         * 内存数据Spill Disk文件
         * */
        private File file;
        /**
         * Shuffle生成TempLocalBlockId
         * */
        private BlockId blockId;
        /**
         * 每次Spill Disk batch长度
         * */
        private List<Long> batchSizes;
        /**
         * 记录Batch文件的偏移量, 第一个元素为0
         * */
        private ArrayList<Long> batchOffsets;

        private int batchIndex = 0;
        private FileChannel fileChannel = null;

        private DeserializationStream deserializeStream;
        private Tuple2<K, C> nextItem;
        private int objectsRead = 0;

        public DiskMapIterator(File file,
                               BlockId blockId,
                               List<Long> batchSizes) {
            this.file = file;
            this.blockId = blockId;
            this.batchSizes = batchSizes;

            this.batchOffsets = Lists.newArrayList();
            long ofs = 0L;
            this.batchOffsets.add(ofs);
            for (long batchSize : batchSizes) {
                ofs += batchSize;
                this.batchOffsets.add(ofs);
            }

            assert file.length() == batchOffsets.get(batchOffsets.size() - 1) :
                                String.format("File length is not equal to the last batch offset:\n" +
                                        " file length = %d\n" +
                                        " last batch offset = %s \n" +
                                        " all batch offsets = %s", file.length(),
                                                                   batchOffsets.get(batchOffsets.size() - 1),
                                                                   StringUtils.join(batchOffsets, '，'));
            this.deserializeStream = nextBatchStream();

            context.addTaskCompletionListener(cxt -> cleanup());
        }

        private DeserializationStream nextBatchStream() {
            try {
                // Note that batchOffsets.length = numBatches + 1 since we did a scan above; check whether
                // we're still in a valid batch.
                if (batchIndex < batchOffsets.size() - 1) {
                    if (deserializeStream != null) {
                        deserializeStream.close();
                        fileChannel.close();
                        deserializeStream = null;
                        fileChannel = null;
                    }
                    // batchOffsets第一个元素放置0
                    long start = batchOffsets.get(batchIndex);
                    fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ);
                    fileChannel.position(start);
                    batchIndex += 1;

                    long end = batchOffsets.get(batchIndex);

                    assert end >= start : "start = " + start + ", end = " + end +
                            ", batchOffsets = " + StringUtils.join(batchOffsets, ',');

                    BufferedInputStream bufferedStream = new BufferedInputStream(ByteStreams.limit(Channels.newInputStream(fileChannel), end - start));
                    InputStream wrappedStream = serializerManager.wrapStream(blockId, bufferedStream);
                    return ser.deserializeStream(wrappedStream);
                } else {
                    // No more batches left
                    cleanup();
                    return null;
                }
            } catch (IOException e) {
                throw new SparkException("read shuffle file " + file.getName() + " failure", e);
            }
        }

        private Tuple2<K, C> readNextItem() {
            try {
                K key = deserializeStream.readKey();
                C collection = deserializeStream.readValue();
                objectsRead += 1;
                if (objectsRead == serializerBatchSize) {
                    deserializeStream = nextBatchStream();
                }
                return new Tuple2<>(key, collection);
            } catch (Exception e) {
                cleanup();
                return null;
            }
        }

        @Override
        public boolean hasNext() {
            if (nextItem == null) {
                if (deserializeStream == null) {
                    return false;
                }
                nextItem = readNextItem();
            }
            return nextItem != null;
        }

        @Override
        public Tuple2<K, C> next() {
            Tuple2<K, C> item = nextItem == null ? readNextItem() : nextItem;
            if (item == null) {
                throw new NoSuchElementException();
            }
            nextItem = null;
            return item;
        }

        private void cleanup() {
            try {
                batchIndex = batchOffsets.size(); // Prevent reading any other batch
                DeserializationStream ds = deserializeStream;
                if (ds != null) {
                    ds.close();
                    deserializeStream = null;
                }
                if (fileChannel != null) {
                    fileChannel.close();
                    fileChannel = null;
                }
                if (file.exists()) {
                    if (!file.delete()) {
                        LOGGER.error("Error deleting {}", file);
                    }
                }
            } catch (IOException e) {
                LOGGER.error("close failure", e);
            }
        }

    }

    private class SpillableIterator implements Iterator<Tuple2<K, C>> {

        private Iterator<Tuple2<K, C>> upStream;

        private final Object SPILL_LOCK = new Object();

        private Iterator<Tuple2<K, C>> nextUpstream;

        private Tuple2<K, C> cur;

        private boolean hasSpilled = false;

        SpillableIterator(Iterator<Tuple2<K, C>> upstream) {
            this.upStream = upstream;
            this.cur = readNext();
        }

        private boolean spill() {
            synchronized (SPILL_LOCK) {
                if (hasSpilled) {
                    return false;
                } else {
                    LOGGER.info("Task {} force spilling in-memory map to disk and it will release {} memory",
                                context.taskAttemptId(), Utils.bytesToString(getUsed()));
                    nextUpstream = spillMemoryIteratorToDisk(upStream);
                    hasSpilled = true;
                    return true;
                }
            }
        }

        private Tuple2<K, C> readNext() {
            synchronized (SPILL_LOCK) {
                if (nextUpstream != null) {
                    upStream = nextUpstream;
                    nextUpstream = null;
                }
                if (upStream.hasNext()) {
                    return upStream.next();
                }
                return null;
            }
        }

        @Override
        public boolean hasNext() {
            return cur != null;
        }

        @Override
        public Tuple2<K, C> next() {
            Tuple2<K, C> r = cur;
            cur = readNext();
            return r;
        }
    }

    private class HashComparator implements Comparator<K> {

        private int hash(Object obj) {
            return obj == null ? 0 : obj.hashCode();
        }

        @Override
        public int compare(K o1, K o2) {
            int hash1 = hash(o1);
            int hash2 = hash(o2);
            return hash1 < hash2 ? -1 : hash1 == hash2 ? 0 : 1;
        }
    }

    private class Updater implements AppendOnlyMap.Updater<C> {

        private Product2<K, V> curEntry;

        public Updater(Product2<K, V> curEntry) {
            this.curEntry = curEntry;
        }

        @Override
        public C updateFunc(boolean hadValue, C value) {
            if (hadValue) {
                return merge.mergeValue(curEntry._2(), value);
            }
            return initial.createCombiner(curEntry._2());
        }
    }
}
