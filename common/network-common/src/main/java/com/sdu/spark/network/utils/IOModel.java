package com.sdu.spark.network.utils;

import com.google.common.base.Strings;

/**
 * @author hanhan.zhang
 * */
public enum IOModel {

    NIO, EPOLL;

    public static IOModel convert(String name) {
        if (Strings.isNullOrEmpty(name)) {
            return NIO;
        }
        for (IOModel model : IOModel.values()) {
            if (model.name().equals(name)) {
                return model;
            }
        }
        return NIO;
    }
}
