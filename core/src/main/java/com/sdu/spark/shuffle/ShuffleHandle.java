package com.sdu.spark.shuffle;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public abstract class ShuffleHandle implements Serializable {

    protected int shuffleId;

    public ShuffleHandle(int shuffleId) {
        this.shuffleId = shuffleId;
    }
}
