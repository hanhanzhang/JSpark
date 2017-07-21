package com.sdu.spark;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public interface SparkApp extends Serializable {

    class TaskSchedulerIsSet implements SparkApp {}

    class StopExecutor implements SparkApp {}

}
