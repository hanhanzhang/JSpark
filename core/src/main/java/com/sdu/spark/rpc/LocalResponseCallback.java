package com.sdu.spark.rpc;

/**
 * @author hanhan.zhang
 * */
public interface LocalResponseCallback<T> {

    void onSuccess(T value);

    void onFailure(Throwable cause);

}
