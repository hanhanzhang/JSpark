package com.sdu.spark;

/**
 * @author hanhan.zhang
 * */
public abstract class MapOutputTracker {

    /**
     *
     * */
    protected long epoch;
    protected Object epochLock = new Object();

}
