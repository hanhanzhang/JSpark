package com.sdu.spark.deploy.worker;

import com.sdu.spark.SecurityManager;
import com.sdu.spark.rpc.RpcEnv;
import com.sdu.spark.rpc.SparkConf;
import com.sdu.spark.utils.Utils;
import org.apache.commons.lang3.ArrayUtils;

import java.lang.reflect.Method;

import static com.sdu.spark.utils.Utils.localHostName;

/**
 * @author hanhan.zhang
 * */
public class DriverWrapper {

    public static void main(String[] args) throws Exception {
        if (ArrayUtils.isEmpty(args)) {
            throw new RuntimeException("argument empty");
        }
        String workerUrl = null, mainClass = null, extraArgs = null;
        for (String arg : args) {
            String[] p = arg.split(" ");
            switch (p[0]) {
                case "--worker-url":
                    workerUrl = p[1];
                    break;
                case "--mainClass":
                    mainClass = p[1];
                    break;
                case "--extraArgs":
                    extraArgs = p[1];
                    break;
            }
        }

        SparkConf conf = new SparkConf();
        RpcEnv rpcEnv = RpcEnv.create("Driver",
                                      localHostName(),
                                      0,
                                      conf,
                                      new SecurityManager(conf));
        rpcEnv.setRpcEndPointRef("workerWatcher", new WorkerWatcher(rpcEnv, workerUrl));

        // Delegate to supplied main class
        Class<?> clazz = Utils.classForName(mainClass);
        Method mainMethod = clazz.getMethod("main", String[].class);
        if (extraArgs == null) {
            return;
        }
        String []executeArgs = extraArgs.split(",");
        mainMethod.invoke(null, executeArgs);

        rpcEnv.shutdown();

    }

}

