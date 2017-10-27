package com.sdu.spark.utils.scala;

import java.util.NoSuchElementException;
import com.sdu.spark.utils.scala.Action.*;

/**
 * @author hanhan.zhang
 * */
public final class RightProjection<A, B> {

    private Either<A, B> e;

    public RightProjection(Either<A, B> e) {
        this.e = e;
    }

    public B get() {
        if (e.isRight()) {
            return ((Right<A, B>) e).e;
        }
        throw new NoSuchElementException("Either.right.value on Left");
    }

    public <U> U foreach(ForeachAction<B, U> action) {
        if (e.isRight()) {
            return action.foreach(((Right<A, B>) e).e);
        }
        return null;
    }

    public B getOrDefault(DefaultAction<B> action) {
        if (e.isRight()) {
            return ((Right<A, B>) e).e;
        }
        return action.or();
    }

}
