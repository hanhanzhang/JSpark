package com.sdu.spark.utils;

import com.sdu.spark.rpc.RpcAddress;
import com.sdu.spark.rpc.RpcEnv;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.rpc.RpcEndPointRef;

/**
 * @author hanhan.zhang
 * */
public class RpcUtils {

    public static RpcEndPointRef makeDriverRef(String name, SparkConf conf, RpcEnv rpcEnv) {
        String driverHost = conf.get("spark.driver.host", "localhost");
        int driverPort = conf.getInt("spark.driver.port", 7077);
        return rpcEnv.setRpcEndPointRef(name, new RpcAddress(driverHost, driverPort));
    }

}
