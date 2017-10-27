package com.sdu.spark.utils.scala;

/**
 * @author hanhan.zhang
 * */
public interface Action {

    interface ForeachAction<A, U> {
        U foreach(A ele);
    }

    interface DefaultAction<A> {
        A or();
    }
}
