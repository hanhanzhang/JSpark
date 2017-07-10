package com.sdu.spark.deploy;

import com.sdu.spark.laucher.SparkSubmitArguments;
import com.sdu.spark.utils.MutableURLClassLoader;

import java.io.File;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static com.sdu.spark.utils.Utils.resolveURI;

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

    private static int CLASS_NOT_FOUND_EXIT_STATUS = 101;

    /******************************Spark集群提交Job*****************************/
    public static void submit(SparkSubmitArguments appArgs) {
        runMain(appArgs.childArgs.toArray(new String[]{}), appArgs.jars.split(","),
                Collections.emptyMap(), appArgs.mainClass);
    }

    private static void runMain(String[] childArgs, String[] childClasspath, Map<String, String> sysProps, String childMainClass) {
        // 加载依赖Jar
        MutableURLClassLoader loader = new MutableURLClassLoader(new URL[childClasspath.length], Thread.currentThread().getContextClassLoader());
        Thread.currentThread().setContextClassLoader(loader);
        Arrays.stream(childClasspath).forEach(jar -> addJarToClasspath(jar, loader));

        if (sysProps != null && !sysProps.isEmpty()){
            sysProps.forEach(System::setProperty);
        }

        // 运行Application Main方法
        Class<?> mainClass = null;
        try {
            mainClass = Class.forName(childMainClass, true, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            System.exit(CLASS_NOT_FOUND_EXIT_STATUS);
        }
        try {
            Method mainMethod = mainClass.getMethod("main", String[].class);
            mainMethod.invoke(null, childArgs);
        } catch (Exception e) {
            System.exit(-1);
        }

    }

    private static void addJarToClasspath(String loadJar, MutableURLClassLoader loader) {
        URI uri = resolveURI(loadJar);
        if (uri.getScheme().equals("file") || uri.getScheme().equals("local")) {
            File file = new File(uri.getPath());
            if (file.exists()) {
                try {
                    loader.addURL(file.toURI().toURL());
                } catch (MalformedURLException e) {
                    // ignore
                }
            } else {
                printStream.println(String.format("Jar文件不存在, Jar = %s", file));
            }
        } else {
            printStream.println(String.format("网络Jar文件跳过, URI = %s", uri));
        }
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
