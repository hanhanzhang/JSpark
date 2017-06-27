package com.sdu.spark.utils;

import com.google.common.collect.Lists;
import com.sdu.spark.SecurityManager;
import com.sdu.spark.deploy.Command;
import com.sdu.spark.laucher.WorkerCommandBuilder;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.sdu.spark.SecurityManager.ENV_AUTH_SECRET;
import static com.sdu.spark.utils.Utils.libraryPathEnvName;

/**
 *
 * @author hanhan.zhang
 * */
public class CommandUtils {

    public static ProcessBuilder buildProcessBuilder(Command command, SecurityManager securityManager, int memory,
                                                     String sparkHome, String[] classPath, String jarPath, Map<String, String> env) throws IOException {
        Command localCommand = buildLocalCommand(command, securityManager, classPath, jarPath, env);
        String[] commandSeq = buildCommandSeq(localCommand, memory, sparkHome);
        ProcessBuilder builder = new ProcessBuilder(commandSeq);
        builder.environment().putAll(localCommand.environment);
        return builder;
    }

    /**
     * 环境变量
     * */
    private static Command buildLocalCommand(Command command, SecurityManager securityManager,
                                             String[] classPath, String jarPath, Map<String, String> env) {
        String libraryPathName = libraryPathEnvName();
        String[] libraryPathEntries = command.libraryPathEntries;
        String cmdLibraryPath = command.environment.get(libraryPathName);

        command.classPathEntries = ArrayUtils.addAll(command.classPathEntries, classPath);
        command.arguments = ArrayUtils.add(command.arguments, jarPath);

        if (ArrayUtils.isNotEmpty(libraryPathEntries) && StringUtils.isNotEmpty(libraryPathName)) {
            List<String> libraryPaths = Lists.newArrayList(libraryPathEntries);
            libraryPaths.add(cmdLibraryPath);
            libraryPaths.add(env.get(libraryPathName));
            command.environment.put(libraryPathName, StringUtils.join(libraryPaths, File.separator));
        }

        if (securityManager.isAuthenticationEnabled()) {
            command.environment.put(ENV_AUTH_SECRET, securityManager.getSecretKey());
        }

        return command;
    }

    private static String[] buildCommandSeq(Command command, int memory, String sparkHome) throws IOException {
        List<String> cmd = new WorkerCommandBuilder(sparkHome, memory, command).buildCommand(Collections.emptyMap());
        cmd.add(command.mainClass);
        cmd.addAll(Arrays.asList(command.arguments));
        return cmd.toArray(new String[0]);
    }

    /**
     * 重定向JVM进程日志输出
     * */
    public static void redirectStream(InputStream input, File out) throws IOException{
        FileOutputStream output = new FileOutputStream(out, true);
        new Thread(() -> {
            try {
                Utils.copyStream(input, output, false);
            } catch (IOException e) {
                // ignore
            }
        }).start();
    }

}
