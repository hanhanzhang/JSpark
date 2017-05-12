package com.sdu.spark.rpc.deploy;

/**
 * @author hanhan.zhang
 * */
public interface MasterMessage {

    class CheckForWorkerTimeOut implements MasterMessage {}

}
