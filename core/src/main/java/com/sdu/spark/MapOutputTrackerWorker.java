package com.sdu.spark;

import com.google.common.collect.Maps;
import com.sdu.spark.scheduler.MapStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author hanhan.zhang
 * */
public class MapOutputTrackerWorker extends MapOutputTracker {

    private static final Logger LOGGER = LoggerFactory.getLogger(MapOutputTrackerWorker.class);

    private Map<Integer, List<MapStatus>> mapStatuses = Maps.newConcurrentMap();

    public void updateEpoch(long newEpoch) {
        synchronized (epochLock) {
            if (newEpoch > epoch) {
                LOGGER.info("更新epoch: {}, 清空缓存", newEpoch);
                epoch = newEpoch;
                mapStatuses.clear();
            }
        }
    }

}
