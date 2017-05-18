package com.sdu.spark.deploy;

import java.io.Serializable;

/**
 * 应用运行状态
 *
 * @author hanhan.zhang
 * */
public enum ApplicationState implements Serializable {

    WAITING, RUNNING, FINISHED, FAILED, KILLED, UNKNOWN

}
