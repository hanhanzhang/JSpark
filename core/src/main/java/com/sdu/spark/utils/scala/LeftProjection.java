package com.sdu.spark.utils.scala;

import java.util.NoSuchElementException;
import com.sdu.spark.utils.scala.Action.*;

/**
 * @author hanhan.zhang
 * */
public final class LeftProjection<A, B> {

    private Either<A, B> e;

    public LeftProjection(Either<A, B> e) {
        this.e = e;
    }

    public A get() {
        if (e.isLeft()) {
            return ((Left<A, B>) e).e;
        }
        throw new NoSuchElementException("Either.left.value on Right");
    }

    public <U> U foreach(ForeachAction<A, U> action) {
        if (e.isLeft()) {
            return action.foreach(((Left<A, B>) e).e);
        }
        return null;
    }

    public A getOrDefault(DefaultAction<A> action) {
        if (e.isLeft()) {
            return ((Left<A, B>) e).e;
        }
        return action.or();
    }
}
