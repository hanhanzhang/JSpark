package com.sdu.spark.deploy;

import lombok.AllArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * 应用运行指令
 *
 * @author hanhan.zhang
 * */
@AllArgsConstructor
public class Command implements Serializable {
    public String mainClass;
    public String[] arguments;
    public Map<String, String> environment;
    public String[] classPathEntries;
    public String[] libraryPathEntries;
    public String[] javaOpts;
}
