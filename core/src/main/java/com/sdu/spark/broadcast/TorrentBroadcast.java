package com.sdu.spark.broadcast;

/**
 * @author hanhan.zhang
 * */
public class TorrentBroadcast<T> extends Broadcast<T> {

    public TorrentBroadcast(long id, T obj) {
        super(id);
    }

    @Override
    protected T getValue() {
        return null;
    }

    @Override
    protected void doDestroy(boolean blocking) {

    }

    @Override
    protected void doUnpersist(boolean blocking) {

    }

    public static void unpersist(long id, boolean removeFromDriver, boolean blocking) {

    }
}
