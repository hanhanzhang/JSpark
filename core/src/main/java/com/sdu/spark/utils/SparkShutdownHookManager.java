package com.sdu.spark.utils;

import java.util.PriorityQueue;

/**
 *
 * @author hanhan.zhang
 * */
public class SparkShutdownHookManager {

    private static PriorityQueue<SparkShutdownHook> hooks = new PriorityQueue<>();


    private static class SparkShutdownHook implements Comparable<SparkShutdownHook> {

        private int priority;

        private Runnable hook;

        public SparkShutdownHook(int priority, Runnable hook) {
            this.priority = priority;
            this.hook = hook;
        }

        @Override
        public int compareTo(SparkShutdownHook o) {
            return o.priority - priority;
        }

        public void run() {
            hook.run();
        }
    }
}
