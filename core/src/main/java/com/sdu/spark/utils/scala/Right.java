package com.sdu.spark.utils.scala;

/**
 * @author hanhan.zhang
 * */
public class Right<A, B> extends Either<A, B> {

    public B e;

    public Right(B b) {
        this.e = b;
    }

    @Override
    public boolean isLeft() {
        return false;
    }

    @Override
    public boolean isRight() {
        return true;
    }
}
