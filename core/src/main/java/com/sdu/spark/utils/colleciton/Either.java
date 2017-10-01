package com.sdu.spark.utils.colleciton;

/**
 * @author hanhan.zhang
 * */
public interface Either<A, B> {

    A left();

    B right();

}
