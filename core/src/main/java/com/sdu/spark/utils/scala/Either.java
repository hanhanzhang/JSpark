package com.sdu.spark.utils.scala;

/**
 * @author hanhan.zhang
 * */
public abstract class Either<A, B> {

    public LeftProjection<A, B> left() {
        return new LeftProjection<>(this);
    }

    public RightProjection<A, B> right() {
        return new RightProjection<>(this);
    }

    public abstract boolean isLeft();

    public abstract boolean isRight();

}
