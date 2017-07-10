package com.sdu.spark;

/**
 * @author hanhan.zhang
 * */
public class TaskKilledException extends RuntimeException {

    public TaskKilledException(String reason) {
        super(reason);
    }
}
