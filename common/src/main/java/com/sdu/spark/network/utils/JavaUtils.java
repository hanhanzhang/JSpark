package com.sdu.spark.network.utils;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author hanhan.zhang
 * */
public class JavaUtils {

    public static void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
            // ignore
        }
    }

}
