package com.sdu.spark.deploy;

import java.io.Serializable;
import java.util.Map;

/**
 * 应用运行指令
 *
 * @author hanhan.zhang
 * */
public class Command implements Serializable {
    public String mainClass;
    public String[] arguments;
    public Map<String, String> environment;
    public String[] classPathEntries;
    public String[] libraryPathEntries;
    public String[] javaOpts;

    public Command(String mainClass, String[] arguments, Map<String, String> environment, String[] classPathEntries, String[] libraryPathEntries, String[] javaOpts) {
        this.mainClass = mainClass;
        this.arguments = arguments;
        this.environment = environment;
        this.classPathEntries = classPathEntries;
        this.libraryPathEntries = libraryPathEntries;
        this.javaOpts = javaOpts;
    }
}
