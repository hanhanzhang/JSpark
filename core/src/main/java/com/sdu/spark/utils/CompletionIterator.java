package com.sdu.spark.utils;

import java.util.Iterator;

/**
 * Wrapper around an iterator which calls a completion method after it successfully iterates
 * through all the elements.
 *
 * @author hanhan.zhang
 * */
public abstract class CompletionIterator<A, I extends Iterator<A>> implements Iterator<A> {

    private I sub;

    private boolean completed = false;

    public CompletionIterator(I sub) {
        this.sub = sub;
        completion();
    }

    @Override
    public boolean hasNext() {
        boolean r = sub.hasNext();
        if (!r) {
            completed = true;
        }
        return r;
    }

    @Override
    public A next() {
        return sub.next();
    }

    /**
     * 遍历结束被调用
     * */
    public abstract void completion();

    public interface IteratorFinished {
        void completionFunction();
    }

    @SuppressWarnings("unchecked")
    public static <A, I extends Iterator<A>> CompletionIterator<A, I> apply(Iterator<A> sub,
                                                                            IteratorFinished iteratorFinished) {
        return new CompletionIterator<A, I>((I) sub) {
            @Override
            public void completion() {
                if (iteratorFinished != null) {
                    iteratorFinished.completionFunction();
                }
            }
        };
    }
}
