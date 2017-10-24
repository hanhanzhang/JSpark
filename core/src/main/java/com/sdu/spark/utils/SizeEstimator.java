package com.sdu.spark.utils;

import com.sdu.spark.utils.sizeof.IObjectProfileNode;
import com.sdu.spark.utils.sizeof.ObjectProfiler;

/**
 * 估算Java Object内存占用量
 *
 * Based on the following JavaWorld article:
 * http://www.javaworld.com/javaworld/javaqa/2003-12/02-qa-1226-sizeof.html
 *
 * @author hanhan.zhang
 * */
public class SizeEstimator {

    public static long estimate(Object obj) {
        IObjectProfileNode profile = ObjectProfiler.profile (obj);
        return profile.size();
    }
}
