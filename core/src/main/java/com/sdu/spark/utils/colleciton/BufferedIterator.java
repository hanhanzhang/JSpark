package com.sdu.spark.utils.colleciton;

import java.util.Iterator;

/**
 * @author hanhan.zhang
 * */
public class BufferedIterator<T> implements Iterator<T> {

    private T hd;

    private Iterator<T> delegate;

    private boolean hdDefined = false;


    public BufferedIterator(Iterator<T> delegate) {
        this.delegate = delegate;
    }

    public T head() {
        if (!hdDefined) {
            hd = next();
            hdDefined = true;
        }
        return hd;
    }

    @Override
    public boolean hasNext() {
        return hdDefined || delegate.hasNext();
    }

    @Override
    public T next() {
        if (hdDefined) {
            hdDefined = false;
            return hd;
        }
        return delegate.next();
    }
}
