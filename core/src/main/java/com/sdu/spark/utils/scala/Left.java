package com.sdu.spark.utils.scala;

/**
 * @author hanhan.zhang
 * */
public final class Left<A, B> extends Either<A, B> {

    public A e;

    public Left(A e) {
        this.e = e;
    }

    @Override
    public boolean isLeft() {
        return true;
    }

    @Override
    public boolean isRight() {
        return false;
    }
}
