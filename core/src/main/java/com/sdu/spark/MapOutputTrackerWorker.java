package com.sdu.spark;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.scheduler.MapStatus;
import com.sdu.spark.shuffle.FetchFailedException;
import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.BlockManagerId;
import com.sdu.spark.utils.scala.Tuple2;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author hanhan.zhang
 * */
public class MapOutputTrackerWorker extends MapOutputTracker {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapOutputTrackerWorker.class);

    // 记录ShuffleId的对应Shuffle结果输出, key = shuffleId, value = Shuffle输出集合
    private Map<Integer, MapStatus[]> mapStatuses = Maps.newConcurrentMap();

    // 记录正在请求ShuffleId对应Shuffle的输出集合(保证线程安全)
    private final Set<Integer> fetching;

    public MapOutputTrackerWorker(SparkConf conf) {
        super(conf);

        this.mapStatuses = Maps.newConcurrentMap();
        this.fetching = Sets.newHashSet();
    }

    public void updateEpoch(long newEpoch) {
        synchronized (epochLock) {
            if (newEpoch > epoch) {
                LOGGER.info("Updating epoch to " + newEpoch + " and clearing cache");
                epoch = newEpoch;
                mapStatuses.clear();
            }
        }
    }

    @Override
    public Multimap<BlockManagerId, Tuple2<BlockId, Long>> getMapSizesByExecutorId(int shuffleId,
                                                                                         int startPartition,
                                                                                         int endPartition) {
        MapStatus[] statuses = getStatus(shuffleId);
        if (statuses == null) {
            return LinkedHashMultimap.create();
        }
        return convertMapStatuses(shuffleId, startPartition, endPartition, statuses);
    }

    @Override
    public void unregisterShuffle(int shuffleId) {
        mapStatuses.remove(shuffleId);
    }

    @Override
    public void stop() {

    }

    private MapStatus[] getStatus(int shuffleId) {
        MapStatus[] statuses = mapStatuses.get(shuffleId);
        if (statuses == null) {
            LOGGER.info("Don't have map outputs for shuffle {}, fetching them", shuffleId);
            long startTime = System.currentTimeMillis();
            MapStatus[] fetchedStatuses;

            // shuffleId对应的Shuffle结果输出空, 则向MapOutputTrackerMaster请求ShuffleId对应的结果输出
            // step1: 判断shuffleId是否已被其他线程请求
            synchronized (fetching) {
                if (fetching.contains(shuffleId)) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }

                fetchedStatuses = mapStatuses.get(shuffleId);
                if (fetchedStatuses == null) {
                    fetching.add(shuffleId);
                }
            }

            // step2: 向MapOutputTrackerMaster请求ShuffleId对应的结果输出
            if (fetchedStatuses == null) {
                LOGGER.info("Doing the fetch; tracker endpoint = {}", trackerEndpoint);
                try {
                    byte[] fetchedBytes = (byte[]) askTracker(new MapOutputTrackerMessage.GetMapOutputStatuses(shuffleId));
                    fetchedStatuses = MapOutputTracker.deserializeMapStatuses(fetchedBytes);
                    LOGGER.info("Got the combiner locations");
                    mapStatuses.put(shuffleId, fetchedStatuses);
                } catch (SparkException e) {
                    // ignore
                } finally {
                    synchronized (fetching) {
                        fetching.remove(shuffleId);
                        fetching.notifyAll();
                    }
                }
            }

            LOGGER.debug("Fetching map combiner statuses for shuffle {} took {} ms", shuffleId, System.currentTimeMillis() - startTime);

            if (fetchedStatuses != null) {
                return fetchedStatuses;
            }

            LOGGER.error("Missing all combiner locations for shuffle {}", shuffleId);
            throw new FetchFailedException.MetadataFetchFailedException(shuffleId, -1, "Missing all combiner locations for shuffle " + shuffleId);
        }
        return statuses;
    }
}
