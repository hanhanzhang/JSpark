package com.sdu.spark.utils.colleciton;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.sdu.spark.*;
import com.sdu.spark.Aggregator.CombinerMerge;
import com.sdu.spark.executor.ShuffleWriteMetrics;
import com.sdu.spark.memory.MemoryManager;
import com.sdu.spark.memory.StaticMemoryManager;
import com.sdu.spark.memory.TaskMemoryManager;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.scheduler.LiveListenerBus;
import com.sdu.spark.serializer.*;
import com.sdu.spark.storage.*;
import com.sdu.spark.storage.BlockId.TempShuffleBlockId;
import com.sdu.spark.utils.scala.Product2;
import com.sdu.spark.utils.scala.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;

import static com.sdu.spark.utils.Utils.bytesToString;
import static com.sdu.spark.utils.Utils.require;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.join;

/**
 * Users interact with this class in the following way:
 *
 * 1. Call insertAll() with a set of records.
 *
 * 2. Request an iterator() back to traverse sorted/aggregated records.
 *    - or -
 *    Invoke writePartitionedFile() to create a file containing sorted/aggregated outputs that can be used in Spark's sort shuffle.
 *
 * @author hanhan.zhang
 * */
public class ExternalSorter<K, V, C> extends Spillable<WritablePartitionedPairCollection<K, C>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExternalSorter.class);

    private SparkConf conf;

    private TaskContext context;
    private Aggregator<K, V, C> aggregator;
    private Comparator<K> ordering;

    /***/
    private Partitioner partitioner;
    private int numPartitions;
    private boolean shouldPartitions;

    private BlockManager blockManager;
    private DiskBlockManager diskBlockManager;
    private SerializerManager serializerManager;
    private SerializerInstance serInstance;

    private int fileBufferSize;
    // Spill数据到磁盘过程中, 分Batch写磁盘
    // Merge SpillFile过程中, 分Batch将磁盘数据读入内存
    private long serializerBatchSize;

    private volatile PartitionedAppendOnlyMap<K, C> map;
    private volatile PartitionedPairBuffer<K, C> buffer;

    private long diskBytesSpilled = 0L;

    /**记录Spill内存数据过程中, 使用的最大内存量*/
    private long peakMemoryUsedBytes = 0L;

    private volatile boolean isShuffleSort = false;
    private List<SpilledFile> forceSpillFiles = Lists.newArrayList();
    private List<SpilledFile> spills = Lists.newArrayList();
    private volatile SpillableIterator readingIterator = null;

    private Comparator<K> keyComparator;

    public ExternalSorter(TaskContext context, Aggregator<K, V, C> aggregator, Partitioner partitioner, Comparator<K> ordering) {
        this(context, aggregator, partitioner, ordering, SparkEnv.env.serializer);
    }

    public ExternalSorter(TaskContext context, Aggregator<K, V, C> aggregator, Partitioner partitioner, Comparator<K> ordering, Serializer serializer) {
        super(context.taskMemoryManager());
        this.context = context;
        this.aggregator = aggregator;
        this.partitioner = partitioner;
        this.ordering = ordering;

        // For test
        this.conf = new SparkConf();

        this.conf = SparkEnv.env.conf;
        this.numPartitions = this.partitioner.numPartitions();
        this.shouldPartitions = this.numPartitions > 1;

        this.blockManager = SparkEnv.env.blockManager;
        this.diskBlockManager = blockManager.diskBlockManager;
        this.serializerManager = SparkEnv.env.serializerManager;
        this.serInstance = serializer.newInstance();

        this.fileBufferSize = (int) conf.getSizeAsKb("spark.shuffle.file.buffer", "32K") * 1024;
        this.serializerBatchSize = conf.getLong("spark.shuffle.spill.batchSize", 10000);

        this.map = new PartitionedAppendOnlyMap<>();
        this.buffer = new PartitionedPairBuffer<>();

        this.keyComparator = ordering != null ? ordering : (a, b) -> {
            int h1 = a == null ? 0 : a.hashCode();
            int h2 = b == null ? 0 : b.hashCode();
            return h1 < h2 ? -1 : h1 == h2 ? 0 : 1;
        };
    }

    private Comparator<K> comparator() {
        return ordering == null || aggregator != null ? keyComparator : null;
    }

    @SuppressWarnings("unchecked")
    public void insertAll(Iterator<? extends Product2<K, V>> records) {
        // TODO: stop combining if we find that the reduction factor isn't high
        if (aggregator != null) {
            while (records.hasNext()) {
                addElementsRead();
                Product2<K, V> kv = records.next();
                int partition = getPartition(kv._1());
                // PartitionedAppendOnlyMap
                //  存储KEY: Tuple<Partition, Key>
                //  存储VALUE: Collection
                // 注意: 按照(Partition, Key)聚合Value
                map.changeValue(new Tuple2<>(partition, kv._1()), (hadValue, value) -> {
                    if (hadValue) {
                        return aggregator.combinerAdd.mergeValue(kv._2(), value);
                    } else {
                        return aggregator.combinerCreator.createCombiner(kv._2());
                    }
                });
                // 是否Spill数据
                maybeSpillCollection(true);
            }
        } else {
            while (records.hasNext()) {
                addElementsRead();
                Product2<K, V> kv = records.next();
                int partition = getPartition(kv._1());
                // PartitionedPairBuffer追加元素, 未聚合元素
                buffer.insert(partition, kv._1(), (C) kv._2());
                maybeSpillCollection(false);
            }
        }
    }

    private void maybeSpillCollection(boolean usingMap) {
        long estimatedSize;
        if (usingMap) {
            estimatedSize = map.estimateSize();
            if (maybeSpill(map, estimatedSize)) {
                map = new PartitionedAppendOnlyMap<>();
            }
        } else {
            estimatedSize = buffer.estimateSize();
            if (maybeSpill(buffer, estimatedSize)) {
                buffer = new PartitionedPairBuffer<>();
            }
        }

        if (estimatedSize > peakMemoryUsedBytes) {
            peakMemoryUsedBytes = estimatedSize;
        }
    }

    /**
     * Write all the data added into this ExternalSorter into a file in the disk store. This is
     * called by the SortShuffleWriter.
     *
     * @param blockId block ID to write to. The index file will be blockId.name + ".index".
     * @return array of lengths, in bytes, of each partition of the file (used by map output tracker)
     * */
    public long[] writePartitionedFile(BlockId blockId, File outputFile) {
        // 每个分区
        long[] lengths = new long[numPartitions];
        DiskBlockObjectWriter writer = blockManager.getDiskWriter(blockId, outputFile, serInstance, fileBufferSize, context.taskMetrics().shuffleWriteMetrics());

        if (spills.isEmpty()) {
            // Case where we only have in-memory data
            WritablePartitionedPairCollection<K, C> collection = aggregator != null ?  map : buffer;
            // PartitionedAppendOnlyMap及PartitionedPairBuffer默认按照Partition排序
            WritablePartitionedIterator it = collection.destructiveSortedWritablePartitionedIterator(keyComparator);
            while (it.hasNext()) {
                int partitionId = it.nextPartition();
                while (it.hasNext() && it.nextPartition() == partitionId) {
                    it.writeNext(writer);
                }
                FileSegment segment = writer.commitAndGet();
                lengths[partitionId] = segment.length;
            }
        } else {
            // We must perform merge-sort; get an iterator by partition and write everything directly.
            Iterator<Tuple2<Integer, Iterator<Tuple2<K, C>>>> iterator = partitionedIterator();
            while (iterator.hasNext()) {
                Tuple2<Integer, Iterator<Tuple2<K, C>>> partitionData = iterator.next();
                // 按分区写数据
                int partition = partitionData._1();
                Iterator<Tuple2<K, C>> partitionDataIterator = partitionData._2();
                while (partitionDataIterator.hasNext()) {
                    Tuple2<K, C> element = partitionDataIterator.next();
                    try {
                        writer.write(element._1(), element._2());
                    } catch (IOException e) {
                        throw new SparkException(format("Exception occurred when write (%s, %s) to disk", element._1(), element._2()), e);
                    }
                }
                FileSegment segment = writer.commitAndGet();
                lengths[partition] = segment.length;
            }
        }

        try {
            writer.close();
        } catch (IOException e) {
            // ignore
        }

        return lengths;
    }

    public void stop() {
        spills.forEach(spilledFile -> spilledFile.file.delete());
        spills.clear();
        forceSpillFiles.forEach(spilledFile -> spilledFile.file.delete());
        forceSpillFiles.clear();
        if (map != null || buffer != null) {
            map = null;
            buffer = null;
            releaseMemory();
        }
    }

    private FileSegment flush(DiskBlockObjectWriter writer) {
        FileSegment segment = writer.commitAndGet();
        diskBytesSpilled += segment.length;
        return segment;
    }

    @Override
    public boolean forceSpill() {
        if (isShuffleSort) {
            return false;
        }
        assert readingIterator != null;
        boolean isSpilled = readingIterator.spill();
        if (isSpilled) {
            map = null;
            buffer = null;
        }
        return isSpilled;
    }

    /**
     * Spill our in-memory collection to a sorted file that we can merge later.
     * We add this file into `spilledFiles` to find it later.
     * */
    @Override
    public void spill(WritablePartitionedPairCollection<K, C> collection) {
        WritablePartitionedIterator iterator = collection.destructiveSortedWritablePartitionedIterator(comparator());
        SpilledFile spilledFile = spillMemoryIteratorToDisk(iterator);
        spills.add(spilledFile);
    }

    private SpilledFile spillMemoryIteratorToDisk(WritablePartitionedIterator inMemoryIterator) {
        // Because these files may be read during shuffle, their compression must be controlled by
        // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
        // createTempShuffleBlock here; see SPARK-3426 for more context.
        Tuple2<TempShuffleBlockId, File> tuple = diskBlockManager.createTempShuffleBlock();

        // These variables are reset after each flush
        // List of batch sizes (bytes) in the order they are written to disk(数组索引即为分区)
        List<Long> batchSizes = Lists.newArrayList();
        // How many elements we have in each partition(分区元素数)
        long[] elementsPerPartition = new long[numPartitions];
        long objectsWritten = 0L;
        ShuffleWriteMetrics spillMetric = new ShuffleWriteMetrics();
        DiskBlockObjectWriter writer = blockManager.getDiskWriter(tuple._1(), tuple._2(), serInstance, fileBufferSize, spillMetric);

        boolean success = false;
        try {
            while (inMemoryIterator.hasNext()) {
                // 当前分区
                int partitionId = inMemoryIterator.nextPartition();
                require(partitionId >= 0 && partitionId < numPartitions,
                        format("partition Id: %d should be in the range [0, %d)", partitionId, numPartitions));

                inMemoryIterator.writeNext(writer);
                long elements = elementsPerPartition[partitionId];
                elements += 1;
                elementsPerPartition[partitionId] = elements;
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
            } else {
                writer.revertPartialWritesAndClose();
            }
            success = true;
        } finally {
            try {
                if (success) {
                    writer.close();
                } else {
                    // This code path only happens if an exception was thrown above before we set success;
                    // close our stuff and let the exception be thrown further
                    writer.revertPartialWritesAndClose();
                    if (tuple._2().exists()) {
                        if (!tuple._2().delete()) {
                            LOGGER.warn("Error deleting {}", tuple._2());
                        }
                    }
                }
            } catch (IOException e) {
                // ignore
            }
        }

        return new SpilledFile(tuple._2(), tuple._1(), batchSizes, elementsPerPartition);
    }

    private int getPartition(K key) {
        return shouldPartitions ? this.partitioner.getPartition(key) : 0;
    }

    /**
     * Given a stream of ((partition, key), combiner) pairs *assumed to be sorted by partition ID*,
     * group together the pairs for each partition into a sub-iterator.
     *
     * @param data an iterator of elements, assumed to already be sorted by partition ID
     * @return (partition, iterator[key, combiner])
     * */
    private Iterator<Tuple2<Integer, Iterator<Tuple2<K, C>>>> groupByPartition(Iterator<Tuple2<Tuple2<Integer, K>, C>> data) {
        List<Tuple2<Integer, Iterator<Tuple2<K, C>>>> partitionData = Lists.newLinkedList();
        BufferedIterator<Tuple2<Tuple2<Integer, K>, C>> bufferedIterator = new BufferedIterator<>(data);
        for (int p = 0 ; p < numPartitions; ++p) {
            partitionData.add(new Tuple2<>(p, new IteratorForPartition(p, bufferedIterator)));
        }
        return partitionData.iterator();
    }

    private Iterator<Tuple2<Tuple2<Integer, K>, C>> destructiveIterator(Iterator<Tuple2<Tuple2<Integer, K>, C>> memoryIterator) {
        if (isShuffleSort) {
            return memoryIterator;
        } else {
            readingIterator = new SpillableIterator(memoryIterator);
            return readingIterator;
        }
    }

    /**
     * Merge a sequence of sorted files, giving an iterator over partitions and then over elements
     * inside each partition. This can be used to either write out a new file or return data to
     * the user.
     *
     * Returns an iterator over all the data written to this object, grouped by partition. For each
     * partition we then have an iterator over its contents, and these are expected to be accessed
     * in order (you can't "skip ahead" to one partition without reading the previous one).
     * Guaranteed to return a key-value pair for each partition, in order of partition ID.
     *
     * */
    private Iterator<Tuple2<Integer, Iterator<Tuple2<K, C>>>> merge(List<SpilledFile> spills, Iterator<Tuple2<Tuple2<Integer, K>, C>> inMemory) {
        List<SpillReader> readers = spills.stream().map(SpillReader::new).collect(Collectors.toList());
        BufferedIterator<Tuple2<Tuple2<Integer, K>, C>> inMemBuffered = new BufferedIterator<>(inMemory);
        List<Tuple2<Integer, Iterator<Tuple2<K, C>>>> partitionKeyValues = Lists.newLinkedList();
        // 将内存数据与磁盘数据按照分区聚合
        for (int p = 0; p < numPartitions; ++p) {
            // InMemory按照分区排序
            IteratorForPartition inMemIterator = new IteratorForPartition(p, inMemBuffered);
            List<Iterator<Tuple2<K, C>>> iterators = readers.stream().map(SpillReader::readNextPartition).collect(Collectors.toList());
            iterators.add(inMemIterator);

            if (aggregator != null) {
                partitionKeyValues.add(new Tuple2<>(p, mergeWithAggregation(
                        iterators, aggregator.combinerMerge, keyComparator, ordering != null
                )));
            } else if (ordering != null) {
                partitionKeyValues.add(new Tuple2<>(p, mergeSort(
                        iterators, ordering
                )));
            } else {
                List<Tuple2<K, C>> keyValues = Lists.newLinkedList();
                iterators.forEach(it -> Iterators.addAll(keyValues, it));
                partitionKeyValues.add(new Tuple2<>(p, keyValues.iterator()));
            }
        }
        return partitionKeyValues.iterator();
    }

    private Iterator<Tuple2<K, C>> mergeSort(List<Iterator<Tuple2<K, C>>> iterators,
                                             Comparator<K> comparator) {
        List<BufferedIterator<Tuple2<K, C>>> bufferedIterators = iterators.stream()
                                                                          .filter(Iterator::hasNext)
                                                                          .map(BufferedIterator::new)
                                                                          .collect(Collectors.toList());
        PriorityQueue<BufferedIterator<Tuple2<K, C>>> heap = new PriorityQueue<>((x, y) -> -comparator.compare(x.head()._1(), y.head()._1()));
        bufferedIterators.forEach(heap::add);
        return new Iterator<Tuple2<K, C>>() {
            @Override
            public boolean hasNext() {
                return heap.size() > 0;
            }

            @Override
            public Tuple2<K, C> next() {
                if (hasNext()) {
                    BufferedIterator<Tuple2<K, C>> firstBuf = heap.poll();
                    Tuple2<K, C> firstPair = firstBuf.next();
                    if (firstBuf.hasNext()) {
                        heap.add(firstBuf);
                    }
                    return firstPair;
                }
                throw new NoSuchElementException();
            }
        };
    }

    private Iterator<Tuple2<K, C>> mergeWithAggregation(List<Iterator<Tuple2<K, C>>> iterators,
                                                        CombinerMerge<C> mergeCombines,
                                                        Comparator<K> comparator,
                                                        boolean totalOrder) {
        if (totalOrder) {
            // We have a total ordering, so the objects with the same key are sequential.
            return new Iterator<Tuple2<K, C>>() {

                BufferedIterator<Tuple2<K, C>> sorted = new BufferedIterator<>(mergeSort(iterators, comparator));

                @Override
                public boolean hasNext() {
                    return sorted.hasNext();
                }

                @Override
                public Tuple2<K, C> next() {
                    if (hasNext()) {
                        Tuple2<K, C> elem = sorted.next();
                        K key = elem._1();
                        C collection = elem._2();
                        while (sorted.hasNext() && sorted.head()._1().equals(key)) {
                            Tuple2<K, C> pair = sorted.next();
                            collection = mergeCombines.mergeCombiners(collection, pair._2());
                        }
                        return new Tuple2<>(key, collection);
                    }
                    throw new NoSuchElementException();
                }
            };
        }

        Iterator<Iterator<Tuple2<K, C>>> iterator = new Iterator<Iterator<Tuple2<K, C>>>() {
            BufferedIterator<Tuple2<K, C>> sorted = new BufferedIterator<>(mergeSort(iterators, comparator));

            List<K> keys = Lists.newArrayList();
            List<C> combines = Lists.newArrayList();

            @Override
            public boolean hasNext() {
                return sorted.hasNext();
            }

            @Override
            public Iterator<Tuple2<K, C>> next() {
                if (hasNext()) {
                    keys.clear();
                    combines.clear();

                    Tuple2<K, C> firstPair = sorted.next();
                    keys.add(firstPair._1());
                    combines.add(firstPair._2());
                    K key = firstPair._1();
                    while (sorted.hasNext() && comparator.compare(sorted.head()._1(), key) == 0) {
                        Tuple2<K, C> pair = sorted.next();
                        int i = 0;
                        boolean foundKey = false;
                        while (i < keys.size() && !foundKey) {
                            if (keys.get(i).equals(pair._1())) {
                                C collection = combines.get(i);
                                combines.set(i, mergeCombines.mergeCombiners(collection, pair._2()));
                                foundKey = true;
                            }
                            ++i;
                        }
                        if (!foundKey) {
                            keys.add(pair._1());
                            combines.add(pair._2());
                        }
                    }
                    return new Iterator<Tuple2<K, C>>() {
                        Iterator<K> self = keys.iterator();
                        Iterator<C> that = combines.iterator();

                        @Override
                        public boolean hasNext() {
                            return self.hasNext() && that.hasNext();
                        }

                        @Override
                        public Tuple2<K, C> next() {
                            return new Tuple2<>(self.next(), that.next());
                        }
                    };
                }
                throw new NoSuchElementException();
            }
        };

        List<Tuple2<K, C>> tupleList = Lists.newLinkedList();
        while (iterator.hasNext()) {
            Iterators.addAll(tupleList, iterator.next());
        }
        return tupleList.iterator();
    }

    /**
     * Return an iterator over all the data written to this object, grouped by partition and
     * aggregated by the requested aggregator. For each partition we then have an iterator over its
     * contents, and these are expected to be accessed in order (you can't "skip ahead" to one
     * partition without reading the previous one). Guaranteed to return a key-value pair for each
     * partition, in order of partition ID.
     * */
    private Iterator<Tuple2<Integer, Iterator<Tuple2<K, C>>>> partitionedIterator() {
        boolean usingMap = aggregator != null;
        WritablePartitionedPairCollection<K, C> collection = usingMap ? map : buffer;
        if (spills.isEmpty()) {
            // Special case: if we have only in-memory data, we don't need to merge streams, and perhaps
            // we don't even need to sort by anything other than partition ID
            if (ordering == null) {
                // The user hasn't requested sorted keys, so only sort by partition ID, not key
                return groupByPartition(destructiveIterator(collection.partitionedDestructiveSortedIterator(null)));
            } else {
                // We do need to sort by both partition ID and key
                return groupByPartition(destructiveIterator(collection.partitionedDestructiveSortedIterator(keyComparator)));
            }
        } else {
            // Merge spilled and in-memory data
            Iterator<Tuple2<Tuple2<Integer, K>, C>> inMemoryIterator = collection.partitionedDestructiveSortedIterator(keyComparator);
            return merge(spills, inMemoryIterator);
        }
    }

    /**Return an iterator over all the data written to this object, aggregated by our aggregator.*/
    public Iterator<Tuple2<K, C>> iterator() {
        isShuffleSort = false;
        Iterator<Tuple2<Integer, Iterator<Tuple2<K, C>>>> partitionedIterator = partitionedIterator();
        // 相同Key分在同一个分区
        List<Tuple2<K, C>> kvList = Lists.newLinkedList();
        while (partitionedIterator.hasNext()) {
            Iterator<Tuple2<K, C>> partitionData = partitionedIterator.next()._2();
            Iterators.addAll(kvList, partitionData);
        }
        return kvList.iterator();
    }

    private class IteratorForPartition implements Iterator<Tuple2<K, C>> {

        int partitionId;
        BufferedIterator<Tuple2<Tuple2<Integer, K>, C>> data;

        IteratorForPartition(int partitionId, BufferedIterator<Tuple2<Tuple2<Integer, K>, C>> data) {
            this.partitionId = partitionId;
            this.data = data;
        }

        @Override
        public boolean hasNext() {
            return data.hasNext() && data.head()._1()._1() == partitionId;
        }

        @Override
        public Tuple2<K, C> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Tuple2<Tuple2<Integer, K>, C> elem = data.next();
            return new Tuple2<>(elem._1()._2(), elem._2());
        }
    }

    private class SpillReader {
        SpilledFile spill;
        // 每个Batch在SpilledFile中偏移量, batchOffsets[batchOffsets.length -1]即SpilledFile长度
        long[] batchOffsets;
        // Batch序号
        int batchId = 0;

        // Track which partition and which batch stream we're in. These will be the indices of
        // the next element we will read. We'll also store the last partition read so that
        // readNextPartition() can figure out what partition that was from.
        int partitionId = 0;
        // 标识每个Partition读取元素个数
        long indexInPartition = 0L;
        // 记录每个Batch读取元素个数
        int indexInBatch = 0;
        int lastPartitionId = 0;

        DeserializationStream deserializeStream;
        FileChannel fileChannel;

        Tuple2<K, C> nextItem;
        boolean finished = false;

        SpillReader(SpilledFile spill) {
            this.spill = spill;
            // 每个Batch在SpilledFile(每次Spill产生一个临时文件)中偏移量(记录serializerBatchSizes.size()的下个位置偏移量)
            batchOffsets = new long[spill.serializerBatchSizes.size() + 1];
            long offset = 0L;
            int i = 0;
            for (; i < spill.serializerBatchSizes.size(); ++i) {
                batchOffsets[i] = offset;
                offset += spill.serializerBatchSizes.get(i);
            }
            batchOffsets[i] = offset;

            // step1: 选择要读取的分区
            skipToNextPartition();
            // step2:
            deserializeStream = nextBatchStream();
        }

        DeserializationStream nextBatchStream() {
            try {
                // batchOffsets多记录下Batch的Offset
                if (batchId < batchOffsets.length - 1) {
                    if (deserializeStream != null) {
                        deserializeStream.close();
                        deserializeStream = null;
                        fileChannel.close();
                        fileChannel = null;
                    }
                    long offset = batchOffsets[batchId];
                    fileChannel = FileChannel.open(spill.file.toPath(), StandardOpenOption.READ);
                    fileChannel.position(offset);
                    batchId += 1;
                    long end = batchOffsets[batchId];

                    // 读取数据
                    BufferedInputStream bufferedStream = new BufferedInputStream(
                            ByteStreams.limit(Channels.newInputStream(fileChannel), end - offset)
                    );
                    InputStream wrappedStream = serializerManager.wrapStream(spill.blockId, bufferedStream);
                    return serInstance.deserializeStream(wrappedStream);
                } else {
                    cleanup();
                    return null;
                }
            } catch (IOException e) {
                throw new SparkException(e);
            }
        }

        // 跳到下个分区读取数据
        // 若是分区Spilled Element Number == 分区已读取数, 则需要开始读取下个分区
        private void skipToNextPartition() {
            while (partitionId < numPartitions &&
                    indexInPartition == spill.elementsPerPartition[partitionId]) {
                partitionId += 1;
                indexInPartition = 0L;
            }
        }

        // TODO: readNextItem是否按照分区读取
        private Tuple2<K, C> readNextItem() {
            if (finished || deserializeStream == null) {
                return null;
            }
            K k = deserializeStream.readKey();
            C c = deserializeStream.readValue();
            lastPartitionId = partitionId;

            // Start reading the next batch if we're done with this one
            indexInBatch += 1;
            if (indexInBatch == serializerBatchSize) {
                // 开始下个Batch数据读取
                indexInBatch = 0;
                deserializeStream = nextBatchStream();
            }

            //  Update the partition location of the element we're reading
            indexInPartition += 1;
            skipToNextPartition();
            // If we've finished reading the last partition, remember that we're done
            if (partitionId == numPartitions) {
                finished = true;
                if (deserializeStream != null) {
                    try {
                        deserializeStream.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
            return new Tuple2<>(k, c);
        }

        int nextPartitionToRead = 0;

        Iterator<Tuple2<K, C>> readNextPartition() {
            // myPartition表示要读取Spill文件分区的数据
            int myPartition = nextPartitionToRead;
            nextPartitionToRead += 1;
            return new Iterator<Tuple2<K, C>>() {
                @Override
                public boolean hasNext() {
                    if (nextItem == null) {
                        nextItem = readNextItem();
                        if (nextItem == null) {
                            return false;
                        }
                    }

                    return lastPartitionId == myPartition;
                }

                @Override
                public Tuple2<K, C> next() {
                    if (hasNext()) {
                        Tuple2<K, C> item = nextItem;
                        nextItem = null;
                        return item;
                    }
                    throw new NoSuchElementException();
                }
            };
        }

        private void cleanup() throws IOException {
            batchId = batchOffsets.length;  // Prevent reading any other batch
            DeserializationStream ds = deserializeStream;
            deserializeStream = null;
            fileChannel = null;
            if (ds != null) {
                ds.close();
            }
        }
    }

    private class SpillableIterator implements Iterator<Tuple2<Tuple2<Integer, K>, C>> {

        private final Object SPILL_LOCK = new Object();

        Iterator<Tuple2<Tuple2<Integer, K>, C>> upStream;

        Iterator<Tuple2<Tuple2<Integer, K>, C>> nextUpStream;

        Tuple2<Tuple2<Integer, K>, C> cur;

        boolean hasSpilled = false;

        SpillableIterator(Iterator<Tuple2<Tuple2<Integer, K>, C>> upStream) {
            this.upStream = upStream;

            cur = readNext();
        }

        boolean spill() {
            synchronized (SPILL_LOCK) {
                if (hasSpilled) {
                    return false;
                }
                WritablePartitionedIterator inMemoryIterator = new WritablePartitionedIterator() {
                    @Override
                    public void writeNext(DiskBlockObjectWriter writer) {
                        try {
                            writer.write(cur._1(), cur._2());
                        } catch (IOException e) {
                            throw new SparkException(format("Exception occurred when write (%s, %s) to disk", cur._1(), cur._2()), e);
                        }
                        cur = upStream.hasNext() ? upStream.next() : null;
                    }

                    @Override
                    public boolean hasNext() {
                        return cur != null;
                    }

                    @Override
                    public int nextPartition() {
                        return cur._1()._1();
                    }
                };
                LOGGER.info("Task {} force spilling in-memory map to disk and it will release {} memory",
                            context.taskAttemptId(), bytesToString(getUsed()));
                SpilledFile spillFile = spillMemoryIteratorToDisk(inMemoryIterator);
                forceSpillFiles.add(spillFile);

                SpillReader spillReader = new SpillReader(spillFile);
                List<Tuple2<Tuple2<Integer, K>, C>> partitionKeyValues = Lists.newLinkedList();
                for (int i = 0; i < numPartitions; ++i) {
                    Iterator<Tuple2<K, C>> iterator = spillReader.readNextPartition();
                    final int partitionId  = i;
                    Iterator<Tuple2<Tuple2<Integer, K>, C>> it = Iterators.transform(iterator, input ->
                            new Tuple2<>(new Tuple2<>(partitionId, input._1()), input._2())
                    );
                    Iterators.addAll(partitionKeyValues, it);
                }

                nextUpStream = partitionKeyValues.iterator();

                hasSpilled = true;
                return true;
            }
        }

        @Override
        public boolean hasNext() {
            return cur != null;
        }

        Tuple2<Tuple2<Integer, K>, C> readNext() {
            synchronized (SPILL_LOCK) {
                if (nextUpStream != null) {
                    upStream = nextUpStream;
                    nextUpStream = null;
                }
                return upStream.hasNext() ? upStream.next() : null;
            }
        }

        @Override
        public Tuple2<Tuple2<Integer, K>, C> next() {
            Tuple2<Tuple2<Integer, K>, C> r = cur;
            cur = readNext();
            return r;
        }
    }

    private class SpilledFile implements Serializable {
        File file;
        BlockId blockId;
        // 记录Spill Batch Size: 用于计算在file中偏移量
        List<Long> serializerBatchSizes;
        // 记录每个分区Spill的元素数
        long[] elementsPerPartition;

        SpilledFile(File file, BlockId blockId, List<Long> serializerBatchSizes, long[] elementsPerPartition) {
            this.file = file;
            this.blockId = blockId;
            this.serializerBatchSizes = serializerBatchSizes;
            this.elementsPerPartition = elementsPerPartition;
        }
    }


    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
        // 设置Spill阈值(即读取元素X个元素发生Spill操作)
        sparkConf.set("spark.shuffle.spill.numElementsForceSpillThreshold", "3");
        // 设置Spill Batch(Spill过程中, 当前待Spill的元素超过X时落地磁盘)
        sparkConf.set("spark.shuffle.spill.batchSize", "2");
        // 配置Shuffle文件目录
        sparkConf.set("spark.local.dir", "/Users/hanhan.zhang/data/spark");

        // SparkEnv
        sparkConf.set("spark.driver.host", "localhost");
        sparkConf.set("spark.driver.port", "7821");
        SparkEnv.env = SparkEnv.createDriverEnv(sparkConf, true, new LiveListenerBus(sparkConf), Runtime.getRuntime().availableProcessors(), null);

        // 内存管理
        MemoryManager memoryManager = new StaticMemoryManager(sparkConf, Runtime.getRuntime().availableProcessors());
        TaskMemoryManager taskMemoryManager = new TaskMemoryManager(memoryManager, 20);

        TaskContext context = new TaskContextImpl(10, 1, 21, 22, taskMemoryManager, new Properties());
        Aggregator.CombinerCreator<Integer, List<Integer>> combinerCreator = Lists::newArrayList;
        Aggregator.CombinerAdd<Integer, List<Integer>> combinerAdd = (v, c) -> {
            c.add(v);
            return c;
        };
        Aggregator.CombinerMerge<List<Integer>> combinerMerge = (c1, c2) -> {
            if (c1 != null) {
                if (c2 != null && !c2.isEmpty()) {
                    c1.addAll(c2);
                    return c1;
                }
                return c1;
            }
            return c2;
        };
        Aggregator<String, Integer, List<Integer>> aggregator = new Aggregator<>(combinerCreator, combinerAdd, combinerMerge);
        Partitioner partitioner = new Partitioner.HashPartitioner(3);
        Serializer serializer = new JavaSerializer(sparkConf);
        ExternalSorter<String, Integer, List<Integer>> sorter = new ExternalSorter<>(context, aggregator, partitioner, null, serializer);

        List<Tuple2<String, Integer>> records = Lists.newLinkedList();
        records.add(new Tuple2<>("A", 1));
        records.add(new Tuple2<>("B", 11));
        records.add(new Tuple2<>("C", 21));
        records.add(new Tuple2<>("D", 31));
        records.add(new Tuple2<>("E", 51));

        records.add(new Tuple2<>("A", 2));
        records.add(new Tuple2<>("B", 12));
        records.add(new Tuple2<>("C", 22));
        records.add(new Tuple2<>("D", 32));
        records.add(new Tuple2<>("E", 52));

        records.add(new Tuple2<>("A", 3));
        records.add(new Tuple2<>("B", 13));
        records.add(new Tuple2<>("C", 23));
        records.add(new Tuple2<>("D", 33));
        records.add(new Tuple2<>("E", 53));
        records.add(new Tuple2<>("E", 55));

        // Spill File
        sorter.insertAll(records.iterator());


        // Merge Spill File ===> Shuffle File
//        int shuffleId = 1, mapId = 2;
//        IndexShuffleBlockResolver shuffleBlockResolver = new IndexShuffleBlockResolver(sparkConf, SparkEnv.env.blockManager);
//        File output = shuffleBlockResolver.getDataFile(shuffleId, mapId);
//        File tmp = Utils.tempFileWith(output);
//        BlockId.ShuffleBlockId blockId = new BlockId.ShuffleBlockId(shuffleId, mapId, 0);
//        long[] partitionLengths = sorter.writePartitionedFile(blockId, tmp);
//        shuffleBlockResolver.writeIndexFileAndCommit(shuffleId, mapId, partitionLengths, tmp);
        Iterator<Tuple2<String, List<Integer>>> iterator = sorter.iterator();
        while (iterator.hasNext()) {
            Tuple2<String, List<Integer>> tuple = iterator.next();
            System.out.println("Key: " + tuple._1() + ", Value: " + join(tuple._2(), ","));
        }

    }
}

