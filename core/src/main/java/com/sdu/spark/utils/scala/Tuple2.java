package com.sdu.spark.utils.scala;

/**
 * @author hanhan.zhang
 * */
public class Tuple2<T1, T2> extends Product2<T1, T2> {

    public Tuple2(T1 _1, T2 _2) {
        super(_1, _2);
    }

    @Override
    public boolean canEqual(Object that) {
        return that instanceof Tuple2;
    }

    @Override
    public String toString() {
        return String.format("(%s, %s)", _1(), _2());
    }
}
