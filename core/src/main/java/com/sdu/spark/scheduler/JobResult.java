package com.sdu.spark.scheduler;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public interface JobResult extends Serializable {

    class JobSucceeded implements JobResult {}

    class JobFailed implements JobResult {
        public Exception exception;

        public JobFailed(Exception exception) {
            this.exception = exception;
        }
    }

}
