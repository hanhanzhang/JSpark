package com.sdu.spark.deploy;

/**
 * @author hanhan.zhang
 * */
public interface MasterMessage {

    class CheckForWorkerTimeOut implements MasterMessage {}

}
