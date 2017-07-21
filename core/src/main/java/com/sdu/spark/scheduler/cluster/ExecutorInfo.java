package com.sdu.spark.scheduler.cluster;

import java.io.Serializable;

/**
 * @author hanhan.zhang
 * */
public class ExecutorInfo implements Serializable {

    public String executorHost;
    public int totalCores;

    public ExecutorInfo(String executorHost, int totalCores) {
        this.executorHost = executorHost;
        this.totalCores = totalCores;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ExecutorInfo that = (ExecutorInfo) o;

        if (totalCores != that.totalCores) return false;
        return executorHost != null ? executorHost.equals(that.executorHost) : that.executorHost == null;

    }

    @Override
    public int hashCode() {
        int result = executorHost != null ? executorHost.hashCode() : 0;
        result = 31 * result + totalCores;
        return result;
    }
}
