package com.sdu.spark.broadcast;

import com.sdu.spark.SparkException;
import com.sdu.spark.utils.Utils;

import java.io.Serializable;

import static java.lang.String.format;

/**
 * @author hanhan.zhang
 * */
public abstract class Broadcast<T> implements Serializable {

    protected long id;

    private volatile boolean isValid = false;
    private volatile String destroySite = "";

    public Broadcast(long id) {
        this.id = id;
    }

    public T value() {
        assertValid();
        return getValue();
    }

    public void unpersist() {
        unpersist(false);
    }

    public void unpersist(boolean blocking) {
        assertValid();
        doUnpersist(blocking);
    }

    public void destroy() {
        destroy(true);
    }

    public void destroy(boolean blocking) {
        assertValid();
        isValid = false;
        destroySite = Utils.getCallSite().shortForm;
        doDestroy(blocking);
    }

    protected abstract T getValue();

    protected abstract void doDestroy(boolean blocking);

    protected abstract void doUnpersist(boolean blocking);

    protected void assertValid() {
        if (!isValid) {
            throw new SparkException(format("Attempted to use %s after it was destroyed (%s)", toString(), destroySite));
        }
    }

    @Override
    public String toString() {
        return "Broadcast(" + id + ")";
    }
}
