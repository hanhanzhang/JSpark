package com.sdu.spark.deploy;

/**
 * @author hanhan.zhang
 * */
public interface WorkerLocalMessage {

    class RegisterWithMaster implements WorkerLocalMessage {}

}
