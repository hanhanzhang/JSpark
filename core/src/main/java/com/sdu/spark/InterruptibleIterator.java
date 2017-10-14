package com.sdu.spark;

import java.util.Iterator;

/**
 * @author hanhan.zhang
 * */
public class InterruptibleIterator<T> implements Iterator<T>{

    private TaskContext context;
    private Iterator<T> delegate;

    public InterruptibleIterator(TaskContext context, Iterator<T> delegate) {
        this.context = context;
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        context.killTaskIfInterrupted();
        return delegate.hasNext();
    }

    @Override
    public T next() {
        return delegate.next();
    }
}
