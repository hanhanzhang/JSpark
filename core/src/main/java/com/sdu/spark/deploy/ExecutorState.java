package com.sdu.spark.deploy;

/**
 * JSpark Executor状态
 *
 * @author hanhan.zhang
 * */
public enum  ExecutorState {

    LAUNCHING, RUNNING, KILLED, FAILED, LOST, EXITED;

    public static boolean isFinshed(ExecutorState state) {
        switch (state) {
            case KILLED:
            case FAILED:
            case LOST:
            case EXITED:
                return true;
            default:
                return false;
        }
    }
}
