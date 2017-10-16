package com.sdu.spark.utils.scala;

import java.util.Iterator;

/**
 * @author hanhan.zhang
 * */
public interface Product extends Equals{

    Object productElement(int n);

    int productArity();

    default Iterator<Object> productIterator() {
        return new Iterator<Object>() {
            int c = 0;
            int max = productArity();
            @Override
            public boolean hasNext() {
                return c < max;
            }

            @Override
            public Object next() {
                Object result = productElement(c);
                c += 1;
                return result;
            }
        };
    }

    default String productPrefix() {
        return "";
    }
}
