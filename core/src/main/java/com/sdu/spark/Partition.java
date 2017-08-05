package com.sdu.spark;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public abstract class Partition implements Serializable {

    public abstract int index();

    @Override
    public int hashCode() {
        return index();
    }
}
