package com.sdu.spark.scheduler;

/**
 * @author hanhan.zhang
 * */
public enum TaskLocality {

    PROCESS_LOCAL, NODE_LOCAL, NO_PREF, RACK_LOCAL, ANY;

    public static boolean isAllowed(TaskLocality constraint, TaskLocality condition) {
        return condition.ordinal() <= constraint.ordinal();
    }
}
