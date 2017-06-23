package com.sdu.spark.deploy;

import lombok.AllArgsConstructor;

import java.io.Serializable;
import java.util.Date;

/**
 *
 * @author hanhan.zhang
 * */
@AllArgsConstructor
public class DriverInfo implements Serializable {

    public long startTime;
    public String id;
    public DriverDescription desc;
    public Date submitDate;

    /********************************无需序列化*******************************/
    public transient DriverState state = DriverState.SUBMITTED;
    public transient Exception exception;
    public transient WorkerInfo worker;

}
