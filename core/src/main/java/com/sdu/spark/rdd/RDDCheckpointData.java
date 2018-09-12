package com.sdu.spark.rdd;

import com.sdu.spark.Partition;

import java.io.Serializable;

import static com.sdu.spark.rdd.CheckpointState.*;

/**
 * @author hanhan.zhang
 * */
public abstract class RDDCheckpointData<T> implements Serializable {

    public transient RDD<T> rdd;

    private CheckpointState cpState;
    private CheckpointRDD<T> cpRdd = null;

    public synchronized boolean isCheckpointed() {
        return cpState == Checkpointed;
    }

    public void checkpoint() {
        synchronized (this) {
            if (cpState == Initialized) {
                cpState = CheckpointingInProgress;
            } else {
                return;
            }
        }

        CheckpointRDD<T> newRDD = doCheckpoint();

        synchronized (this) {
            cpRdd = newRDD;
            cpState = Checkpointed;
            rdd.markCheckpointed();
        }
    }

    public synchronized CheckpointRDD<T> checkpointRDD() {
        return cpRdd;
    }

    public synchronized Partition[] getPartitions() {
        if (cpRdd == null) {
            return new Partition[0];
        }

        return cpRdd.partitions();
    }

    public abstract CheckpointRDD<T> doCheckpoint();
}
