package com.sdu.spark.deploy;

import java.io.Serializable;

/**
 * 应用Driver状态
 *
 * @author hanhan.zhang
 * */
public enum DriverState implements Serializable {

    SUBMITTED, RUNNING, FINISHED, RELAUNCHING, UNKNOWN, KILLED, FAILED, ERROR

}
