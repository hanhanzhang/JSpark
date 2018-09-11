package com.sdu.spark.scheduler;

/**
 * @author hanhan.zhang
 * */
public class TaskInfo {

    private long taskId;
    public int index;
    public int attemptNumber;
    private long launchTime;
    private String executorId;
    private String host;
    private TaskLocality taskLocality;
    private boolean speculative;


    /**
     * The time when the task started remotely getting the result. Will not be set if the
     * task result was sent immediately when the task finished (as opposed to sending an
     * IndirectTaskResult and later fetching the result from the block manager).
     * */
    private long gettingResultTime = 0;

    public TaskInfo(long taskId,
                    int index,
                    int attemptNumber,
                    long launchTime,
                    String executorId,
                    String host,
                    TaskLocality taskLocality,
                    boolean speculative) {
        this.taskId = taskId;
        this.index = index;
        this.attemptNumber = attemptNumber;
        this.launchTime = launchTime;
        this.executorId = executorId;
        this.host = host;
        this.taskLocality = taskLocality;
        this.speculative = speculative;
    }

    public void markGettingResult(long time) {
        gettingResultTime = time;
    }
}
