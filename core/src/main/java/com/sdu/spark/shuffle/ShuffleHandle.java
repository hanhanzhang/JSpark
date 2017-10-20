package com.sdu.spark.shuffle;

import java.io.Serializable;

/**
 * An opaque handle to a shuffle, used by a ShuffleManager to pass information about it to tasks.
 *
 * ShuffleWriter选择标识及用来传递数据
 *
 * @author hanhan.zhang
 * */
public abstract class ShuffleHandle implements Serializable {

    public int shuffleId;

    public ShuffleHandle(int shuffleId) {
        this.shuffleId = shuffleId;
    }
}
