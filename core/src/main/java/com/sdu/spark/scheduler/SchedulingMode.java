package com.sdu.spark.scheduler;

/**
 * @author hanhan.zhang
 * */
public enum SchedulingMode {

    FAIR, FIFO, NONE;

    public static SchedulingMode withName(String name) {
        SchedulingMode[] modes = SchedulingMode.values();
        for (SchedulingMode mode : modes) {
            if (mode.name().equals(name)) {
                return mode;
            }
        }
        throw new UnsupportedOperationException("Unsupported schedule mode : " + name);
    }
}

