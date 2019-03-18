package com.sdu.spark.shuffle.sort;

import com.sdu.spark.ShuffleDependency;
import com.sdu.spark.SparkEnv;
import com.sdu.spark.TaskContext;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.scheduler.MapStatus;
import com.sdu.spark.shuffle.BaseShuffleHandle;
import com.sdu.spark.shuffle.IndexShuffleBlockResolver;
import com.sdu.spark.shuffle.ShuffleWriter;
import com.sdu.spark.storage.BlockId.ShuffleBlockId;
import com.sdu.spark.storage.BlockManager;
import com.sdu.spark.utils.TIterator;
import com.sdu.spark.utils.Utils;
import com.sdu.spark.utils.colleciton.ExternalSorter;
import com.sdu.spark.utils.scala.Product2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static com.google.common.base.Preconditions.checkArgument;
import static com.sdu.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID;

/**
 *
 *                          +-------------------------------------------------------+
 * +-----------+  content   | +------------+   +------------+        +------------+ |
 * | Data File | ---------> | | partition1 |   | partition2 |  ....  | partitionN | |
 * +-----------+            | +------------+   +------------+        +------------+ |
 *                          +------/|\---------------/|\-------------------/|\------+
 *                                  |                 |                     |
 *                          +-------|-----------------|---------------------|-------+
 * +------------+           | +------------+   +------------+        +------------+ |
 * | Index File | --------> | |   offset1  |   |   offset2  |  ....  |  offsetN   | |
 * +------------+           | +------------+   +------------+        +------------+ |
 *                          +-------------------------------------------------------+
 * @author hanhan.zhang
 * */
public class SortShuffleWriter<K, V, C> implements ShuffleWriter<K, V>{

    private static final Logger LOGGER = LoggerFactory.getLogger(SortShuffleWriter.class);

    private IndexShuffleBlockResolver shuffleBlockResolver;
    private int mapId;
    private TaskContext context;

    private boolean stopping = false;
    private BlockManager blockManager;
    private ShuffleDependency<K, V, C> dep;
    private MapStatus mapStatus;

    private ExternalSorter<K, V, C> sorter;

    public SortShuffleWriter(IndexShuffleBlockResolver shuffleBlockResolver, BaseShuffleHandle<K, V, C> handle,
                             int mapId, TaskContext context) {
        this.shuffleBlockResolver = shuffleBlockResolver;
        this.mapId = mapId;
        this.context = context;
        this.dep = handle.shuffleDep();
        this.blockManager = SparkEnv.env.blockManager;
    }

    @Override
    public void write(TIterator<Product2<K, V>> records) {
        if (dep.mapSideCombine) {
            sorter = new ExternalSorter<>(context, dep.aggregator, dep.partitioner, dep.keyOrdering, dep.serializer);
        } else {
            sorter = new ExternalSorter<>(context, null, dep.partitioner, dep.keyOrdering, dep.serializer);
        }
        sorter.insertAll(records);

        // Don't bother including the time to open the merged combinerMerge file in the shuffle write time,
        // because it just opens a single file, so is typically too fast to measure accurately
        // (see SPARK-3570).
        File output = shuffleBlockResolver.getDataFile(dep.shuffleId(), mapId);
        File tmp = Utils.tempFileWith(output);
        try {
            ShuffleBlockId blockId = new ShuffleBlockId(dep.shuffleId(), mapId, NOOP_REDUCE_ID);
            long[] partitionLengths = sorter.writePartitionedFile(blockId, tmp);
            shuffleBlockResolver.writeIndexFileAndCommit(dep.shuffleId(), mapId, partitionLengths, tmp);
            mapStatus = MapStatus.apply(blockManager.shuffleServerId, partitionLengths);
        } finally {
            if (tmp.exists() && !tmp.delete()) {
                LOGGER.error("Error while deleting temp file {}", tmp.getAbsoluteFile());
            }
        }
    }

    @Override
    public MapStatus stop(boolean success) {
        try {
            if (stopping) {
                return null;
            }
            stopping = true;
            if (success) {
                return mapStatus;
            } else {
                return null;
            }
        } finally {
            if (sorter != null) {
                sorter.stop();
                sorter = null;
            }
        }
    }

    public static boolean shouldBypassMergeSort(SparkConf conf, ShuffleDependency<?, ?, ?> dep) {
        if (dep.mapSideCombine) {   // map端需数据聚合
            checkArgument(dep.aggregator == null, "Map-side combine without Aggregator specified!");
            return false;
        }
        int bypassMergeThreshold = conf.getInt("spark.shuffle.sort.bypassMergeThreshold", 200);
        return dep.partitioner.numPartitions() <= bypassMergeThreshold;
    }
}
