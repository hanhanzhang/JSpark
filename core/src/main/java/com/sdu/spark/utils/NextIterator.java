package com.sdu.spark.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author hanhan.zhang
 * */
public abstract class NextIterator<U> implements Iterator<U> {

    private boolean gotNext = false;
    private U nextValue;
    private boolean closed = false;
    protected boolean finished = false;

    @Override
    public boolean hasNext() {
        if (!finished) {
            if (!gotNext) {
                nextValue = getNext();
                if (finished) {
                    closeIfNeeded();
                }
                gotNext = true;
            }
        }
        return  !finished;
    }

    @Override
    public U next() {
        if (!hasNext()) {
            throw new NoSuchElementException("End of stream");
        }
        gotNext = false;
        return nextValue;
    }

   protected void closeIfNeeded() {
        if (!closed) {
            closed = true;
            close();
        }
    }

    public abstract U getNext();

    public abstract void close();
}
