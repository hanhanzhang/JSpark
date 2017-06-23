package com.sdu.spark.utils;

import com.google.common.collect.Lists;
import com.sdu.spark.SecurityManager;
import com.sdu.spark.deploy.Command;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
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
                                                     String sparkHome, String[] classPath, Map<String, String> env) {
        Command localCommand = buildLocalCommand(command, securityManager, classPath, env);
        String[] commandSeq = buildCommandSeq(localCommand, memory, sparkHome);
        ProcessBuilder builder = new ProcessBuilder(commandSeq);
        builder.environment().putAll(localCommand.environment);
        return builder;
    }

    /**
     * 环境变量
     * */
    private static Command buildLocalCommand(Command command, SecurityManager securityManager,
                                             String[] classPath, Map<String, String> env) {
        String libraryPathName = libraryPathEnvName();
        String[] libraryPathEntries = command.libraryPathEntries;
        String cmdLibraryPath = command.environment.get(libraryPathName);

        command.classPathEntries = ArrayUtils.addAll(command.classPathEntries, classPath);

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

    private static String[] buildCommandSeq(Command command, int memory, String sparkHome) {
        return null;
    }

}
