package com.sdu.spark.deploy;

import com.sdu.spark.laucher.SparkSubmitArguments;

import java.io.PrintStream;

/**
 * Spark Task提交入口
 *
 * @author hanhan.zhang
 * */
public class SparkSubmit {

    /*******************************Spark集群管理********************************/
    private int YARN = 1;
    private int STANDALONE = 2;
    private int MESOS = 4;
    private int LOCAL = 8;
    private int ALL_CLUSTER_MGRS = YARN | STANDALONE | MESOS | LOCAL;

    /*******************************Spark部署类型*******************************/
    private int CLIENT = 1;
    private int CLUSTER = 2;
    private int ALL_DEPLOY_MODES = CLIENT | CLUSTER;

    private static PrintStream printStream = System.err;

    /******************************Spark集群提交Job*****************************/
    public static void submit(SparkSubmitArguments appArgs) {

    }

    /******************************Spark集群杀死Job****************************/
    public static void kill(SparkSubmitArguments appArgs) {

    }

    /******************************Spark集群Job状态****************************/
    public static void requestStatus(SparkSubmitArguments appArgs) {

    }

    public static void main(String[] args) {
        SparkSubmitArguments appArgs = new SparkSubmitArguments(args);
        if (appArgs.verbose) {
            printStream.println(appArgs);
        }

        switch (appArgs.action) {
            case SUBMIT:
                submit(appArgs);
                break;
            case KILL:
                kill(appArgs);
                break;
            case REQUEST_STATUS:
                requestStatus(appArgs);
                break;
            default:
                throw new IllegalArgumentException("Unknown submit action : " + appArgs.action);
        }
    }

}
