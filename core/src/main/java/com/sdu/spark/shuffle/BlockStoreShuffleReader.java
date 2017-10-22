package com.sdu.spark.shuffle;

import com.google.common.collect.Lists;
import com.sdu.spark.*;
import com.sdu.spark.serializer.SerializerInstance;
import com.sdu.spark.serializer.SerializerManager;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.BlockManager;
import com.sdu.spark.utils.CompletionIterator;
import com.sdu.spark.utils.colleciton.ExternalSorter;
import com.sdu.spark.utils.scala.Product2;
import com.sdu.spark.utils.scala.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Iterators.transform;
import static org.apache.commons.crypto.utils.Utils.checkArgument;

/**
 *
 * {@link BlockStoreShuffleReader}负责读取{@link #startPartition}~{@link #endPartition} Shuffle数据
 *
 * 1: {@link ShuffleBlockFetcherIterator}负责Executor上读取Block数据
 *
 * 2: {@link #read()}Shuffle数据读取完成后, 根据{@link ShuffleDependency}是否定义{@link Aggregator}进行
 *
 *    Shuffle数据聚合; 根据{@link ShuffleDependency#keyOrdering}裁决是否对数据进行排序
 *
 * TODO: {@link ExternalSorter}待实现排序模块
 *
 * @author hanhan.zhang
 * */
public class BlockStoreShuffleReader<K, C> implements ShuffleReader<K, C> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BlockStoreShuffleReader.class);

    private BaseShuffleHandle<K, Object, C> handle;
    private int startPartition;
    private int endPartition;
    private TaskContext context;

    private SerializerManager serializerManager;
    private BlockManager blockManager;
    private MapOutputTracker mapOutputTracker;

    private ShuffleDependency<K, Object, C> dep;

    public BlockStoreShuffleReader(BaseShuffleHandle<K, Object, C> handle,
                                   int startPartition,
                                   int endPartition,
                                   TaskContext context) {
        this.handle = handle;
        this.startPartition = startPartition;
        this.endPartition = endPartition;
        this.context = context;

        this.serializerManager = SparkEnv.env.serializerManager;
        this.blockManager = SparkEnv.env.blockManager;
        this.mapOutputTracker = SparkEnv.env.mapOutputTracker;

        this.dep = handle.dependency;
    }


    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Product2<K, C>> read() {
        ShuffleBlockFetcherIterator wrappedStreams = new ShuffleBlockFetcherIterator(
                context,
                blockManager.shuffleClient,
                blockManager,
                mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
                (blockId, inputStream) -> serializerManager.wrapStream(blockId, inputStream),
                SparkEnv.env.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
                SparkEnv.env.conf.getInt("spark.reducer.maxReqsInFlight", Integer.MAX_VALUE),
                SparkEnv.env.conf.getInt("spark.reducer.maxBlocksInFlightPerAddress", Integer.MAX_VALUE),
                SparkEnv.env.conf.getLong("spark.reducer.maxReqSizeShuffleToMem", Long.MAX_VALUE),
                SparkEnv.env.conf.getBoolean("spark.shuffle.detectCorrupt", true)
        );

        SerializerInstance ser = dep.serializer.newInstance();
        // Java不支持Iterator.flatMap, 还是Scala方便
        List<Tuple2<Object, Object>> records = Lists.newLinkedList();
        while (wrappedStreams.hasNext()) {
            try {
                InputStream wrappedStream = wrappedStreams.next()._2();
                Iterator<Tuple2<Object, Object>> recIter = ser.deserializeStream(wrappedStream).asKeyValueIterator();
                while (recIter.hasNext()) {
                    records.add(recIter.next());
                }
            } catch (IOException e) {
                String msg = "deserialize failure";
                LOGGER.error(msg, e);
                throw new SparkException(msg, e);
            }
        }
        Iterator<Tuple2<Object, Object>> recordIter = records.iterator();

        // TODO: Metric
        CompletionIterator<Tuple2<Object, Object>, Iterator<Tuple2<Object, Object>>> metricIter = CompletionIterator.apply(
                recordIter,
                null
        );

        // Shuffle Block数据聚合(Merge)
        InterruptibleIterator<Tuple2<Object, Object>> interruptibleIterator = new InterruptibleIterator<>(context, metricIter);
        Iterator<Tuple2<K, C>> aggregatedIter = null;
        if (dep.aggregator != null) {
            if (dep.mapSideCombine) {
                // Map端已聚合相同Key数据
                Iterator<Product2<K, C>> combineKeyValuesIterator = transform(interruptibleIterator, tuple -> (Product2<K, C>) tuple);
                aggregatedIter = dep.aggregator.combineCombinersByKey(combineKeyValuesIterator, context);
            } else {
                // We don't know the value type, but also don't care -- the dependency *should*
                // have made sure its compatible w/ this aggregator, which will convert the value
                // type to the combined type C
                Iterator<Product2<K, Object>> keyValuesIterator = transform(interruptibleIterator, tuple -> (Product2<K, Object>) tuple);
                aggregatedIter = dep.aggregator.combineValueByKey(keyValuesIterator, context);
            }
        } else {
            checkArgument(dep.mapSideCombine, "Map-side combine without Aggregator specified!");
            aggregatedIter = transform(interruptibleIterator, tuple -> (Tuple2<K, C>) tuple);
        }

        // Shuffle Block数据排序(Sort)
        if (dep.keyOrdering) {
            ExternalSorter<K, C, C> sorter = new ExternalSorter<>(
                    context,
                    null,
                    null,
                    true,
                    dep.serializer
            );
            sorter.insertAll(aggregatedIter);
            // TODO: Task Metric
        }

        return transform(aggregatedIter, tuple -> (Product2<K, C>) tuple);
    }

}
