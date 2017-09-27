package com.sdu.spark;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public interface MapOutputTrackerMessage extends Serializable {

    class GetMapOutputStatuses implements MapOutputTrackerMessage {
        int shuffleId;

        public GetMapOutputStatuses(int shuffleId) {
            this.shuffleId = shuffleId;
        }
    }

    class StopMapOutputTracker implements MapOutputTrackerMessage {}

}
