package com.sdu.spark.laucher;

import com.sdu.spark.deploy.Command;
import com.sdu.spark.launcher.AbstractCommandBuilder;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 * @author hanhan.zhang
 * */
public class WorkerCommandBuilder extends AbstractCommandBuilder {

    public String sparkHome;
    public int memoryMb;
    public Command command;

    public WorkerCommandBuilder(String sparkHome, int memoryMb, Command command) {
        this.sparkHome = sparkHome;
        this.memoryMb = memoryMb;
        this.command = command;
        this.childEnv.putAll(command.environment);
        this.childEnv.put(ENV_SPARK_HOME, sparkHome);
    }

    @Override
    public List<String> buildCommand(Map<String, String> env) throws IOException, IllegalArgumentException {
        List<String> cmd = buildJavaCommand(StringUtils.join(this.command.classPathEntries, File.separator));
        cmd.add(String.format("-Xmx%sM", memoryMb));
        cmd.addAll(Arrays.asList(command.javaOpts));
        return cmd;
    }
}
