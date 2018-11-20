package com.sdu.spark.shuffle.sort;

import com.sdu.spark.ShuffleDependency;
import com.sdu.spark.SparkEnv;
import com.sdu.spark.TaskContext;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.shuffle.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.sdu.spark.shuffle.sort.PackedRecordPointer.MAXIMUM_PARTITION_ID;

/**
 * {@link SortShuffleManager}职责:
 *
 * 1: shuffle管理:
 *
 *    shuffle注册({@link #numMapsForShuffle}维护每个shuffle对应map数)
 *
 *    shuffle移除{@link #unregisterShuffle(int)}, 移除shuffle同时删除shuffle的产生文件(数据/索引文件)
 *
 * 2：Shuffle数据写{@link #getWriter(ShuffleHandle, int, TaskContext)}
 *
 * 3: Shuffle数据读{@link #getReader(ShuffleHandle, int, int, TaskContext)}
 *
 * @author hanhan.zhang
 * */
public class SortShuffleManager implements ShuffleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SortShuffleManager.class);

    private static final int MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE = MAXIMUM_PARTITION_ID + 1;

    /**
     * A mapping from shuffle ids to the number of mappers producing combinerMerge for those shuffles
     * */
    private ConcurrentMap<Integer, Integer> numMapsForShuffle = new ConcurrentHashMap<>();

    private SparkConf conf;
    private IndexShuffleBlockResolver shuffleBlockResolver;

    public SortShuffleManager(SparkConf conf) {
        this.conf = conf;
        this.shuffleBlockResolver = new IndexShuffleBlockResolver(conf);

        if (!conf.getBoolean("spark.shuffle.spill", true)) {
            LOGGER.warn("spark.shuffle.spill was set to false, but this configuration is ignored as of Spark 1.6+." +
                        " Shuffle will continue to spill to disk when necessary.");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V, C> ShuffleHandle registerShuffle(int shuffleId,
                                                   int numMaps,
                                                   ShuffleDependency<K, V, C> dependency) {
        if (SortShuffleWriter.shouldBypassMergeSort(conf, dependency)) {
            // Map端需按照Key聚合数据且分区数小于等于'spark.shuffle.sort.bypassMergeThreshold'阈值
            return new BypassMergeSortShuffleHandle<>(shuffleId, numMaps, (ShuffleDependency<K, V, V>) dependency);
        } else if (canUseSerializedShuffle(dependency)) {
            return new SerializedShuffleHandle<>(shuffleId, numMaps, (ShuffleDependency<K, V, V>) dependency);
        } else {
            return new BaseShuffleHandle<>(shuffleId, numMaps, dependency);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> ShuffleWriter<K, V> getWriter(ShuffleHandle handle, int mapId, TaskContext context) {
        numMapsForShuffle.putIfAbsent(handle.shuffleId, ((BaseShuffleHandle) handle).numMaps());
        SparkEnv env = SparkEnv.env;
        if (handle instanceof SerializedShuffleHandle) {
            return new UnsafeShuffleWriter<>(env.blockManager, shuffleBlockResolver, context.taskMemoryManager(),
                    (SerializedShuffleHandle<K, V>) handle, mapId, context, env.conf);
        } else if (handle instanceof BypassMergeSortShuffleHandle) {
            return new BypassMergeSortShuffleWriter<>(env.blockManager, shuffleBlockResolver,
                    (BypassMergeSortShuffleHandle<K, V>) handle, mapId, context, env.conf);
        } else {
            return new SortShuffleWriter<>(shuffleBlockResolver, (BaseShuffleHandle<K, V, ?>) handle, mapId, context);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, C> ShuffleReader<K, C> getReader(ShuffleHandle handle, int startPartition, int endPartition, TaskContext context) {
        return new BlockStoreShuffleReader<>((BaseShuffleHandle<K, Object, C>)handle, startPartition, endPartition, context);
    }

    @Override
    public boolean unregisterShuffle(int shuffleId) {
        int numMaps = numMapsForShuffle.remove(shuffleId);
        for (int mapId = 0; mapId < numMaps; ++mapId) {
            shuffleBlockResolver.removeDataByMap(shuffleId, mapId);
        }
        return true;
    }

    @Override
    public ShuffleBlockResolver shuffleBlockResolver() {
        return shuffleBlockResolver;
    }

    @Override
    public void stop() {
        shuffleBlockResolver.stop();
    }

    private static boolean canUseSerializedShuffle(ShuffleDependency<?, ?, ?> dep) {
        int shuffleId = dep.shuffleId();
        int numPartitions = dep.partitioner.numPartitions();
        if (!dep.serializer.supportsRelocationOfSerializedObjects()) {
            LOGGER.debug("Can't use serialized shuffle for shuffle {} because the serializer, " +
                         "{} does not support object relocation", shuffleId, dep.serializer.getClass().getSimpleName());
            return false;
        } else if (dep.aggregator != null) {
            LOGGER.debug("Can't use serialized shuffle for shuffle {} because an aggregator is defined", shuffleId);
            return false;
        } else if (numPartitions > MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE) {
            LOGGER.debug("Can't use serialized shuffle for shuffle {} because it has more than {} partitions",
                         shuffleId, MAX_SHUFFLE_OUTPUT_PARTITIONS_FOR_SERIALIZED_MODE);
            return false;
        } else {
            LOGGER.debug("Can use serialized shuffle for shuffle {}", shuffleId);
            return true;
        }
    }
}
