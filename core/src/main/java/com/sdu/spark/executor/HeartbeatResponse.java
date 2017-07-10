package com.sdu.spark.executor;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public class HeartbeatResponse implements Serializable {
    public boolean registerBlockManager;
}
