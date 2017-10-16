package com.sdu.spark.utils.scala;

/**
 * @author hanhan.zhang
 * */
public abstract class Product2<T1, T2> implements Product {

    private T1 _1;
    private T2 _2;

    public Product2(T1 _1, T2 _2) {
        this._1 = _1;
        this._2 = _2;
    }

    @Override
    public Object productElement(int n) {
        switch (n) {
            case 1:
                return _1;
            case 2:
                return _2;
            default:
                throw new IndexOutOfBoundsException(String.valueOf(n));
        }
    }

    @Override
    public int productArity() {
        return 2;
    }

    public T1 _1() {
        return _1;
    }

    public T2 _2() {
        return _2;
    }
}
