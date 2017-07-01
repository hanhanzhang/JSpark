package com.sdu.spark.launcher;

import java.io.PrintStream;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Spark Task提交参数解析
 *
 * @author hanhan.zhang
 * */
public abstract class SparkSubmitOptionParser {

    protected final String CLASS = "--class";
    protected final String CONF = "--conf";
    protected final String DEPLOY_MODE = "--deploy-mode";
    protected final String DRIVER_CLASS_PATH = "--driver-class-path";
    protected final String DRIVER_CORES = "--driver-cores";
    protected final String DRIVER_JAVA_OPTIONS =  "--driver-java-options";
    protected final String DRIVER_LIBRARY_PATH = "--driver-library-path";
    protected final String DRIVER_MEMORY = "--driver-memory";
    protected final String EXECUTOR_MEMORY = "--executor-memory";
    protected final String FILES = "--files";
    protected final String JARS = "--jars";
    protected final String KILL_SUBMISSION = "--kill";
    protected final String MASTER = "--master";
    protected final String NAME = "--name";
    protected final String PACKAGES = "--packages";
    protected final String PACKAGES_EXCLUDE = "--exclude-packages";
    protected final String PROPERTIES_FILE = "--properties-file";
    protected final String PROXY_USER = "--proxy-user";
    protected final String PY_FILES = "--py-files";
    protected final String REPOSITORIES = "--repositories";
    protected final String STATUS = "--status";
    protected final String TOTAL_EXECUTOR_CORES = "--total-executor-cores";

    // Options that do not take arguments.
    protected final String HELP = "--help";
    protected final String SUPERVISE = "--supervise";
    protected final String USAGE_ERROR = "--usage-error";
    protected final String VERBOSE = "--verbose";
    protected final String VERSION = "--version";

    // Standalone-only options.

    // YARN-only options.
    protected final String ARCHIVES = "--archives";
    protected final String EXECUTOR_CORES = "--executor-cores";
    protected final String KEYTAB = "--keytab";
    protected final String NUM_EXECUTORS = "--num-executors";
    protected final String PRINCIPAL = "--principal";
    protected final String QUEUE = "--queue";


    // 输出流
    protected PrintStream printStream = System.err;

    final String[][] opts = {
            { ARCHIVES },
            { CLASS },
            { CONF, "-c" },
            { DEPLOY_MODE },
            { DRIVER_CLASS_PATH },
            { DRIVER_CORES },
            { DRIVER_JAVA_OPTIONS },
            { DRIVER_LIBRARY_PATH },
            { DRIVER_MEMORY },
            { EXECUTOR_CORES },
            { EXECUTOR_MEMORY },
            { FILES },
            { JARS },
            { KEYTAB },
            { KILL_SUBMISSION },
            { MASTER },
            { NAME },
            { NUM_EXECUTORS },
            { PACKAGES },
            { PACKAGES_EXCLUDE },
            { PRINCIPAL },
            { PROPERTIES_FILE },
            { PROXY_USER },
            { PY_FILES },
            { QUEUE },
            { REPOSITORIES },
            { STATUS },
            { TOTAL_EXECUTOR_CORES },
    };

    final String[][] switches = {
            { HELP, "-h" },
            { SUPERVISE },
            { USAGE_ERROR },
            { VERBOSE, "-v" },
            { VERSION },
    };

    /********************************解析spark-submit参数*********************************/
    protected final void parse(List<String> args) {
        // [: 标记表达式的开始
        Pattern eqSeparatedOpt = Pattern.compile("(--[^=]+)=(.+)");
        int idx = 0;
        for (idx = 0; idx < args.size(); ++idx) {
            String arg = args.get(idx);
            String value = null;

            Matcher m = eqSeparatedOpt.matcher(arg);
            if (m.matches()) {
                arg = m.group(1);
                value = m.group(2);
            }

            // 使用标准参数[若未找到, 在switches中找]
            String name = findCliOption(arg, opts);
            if (name != null) {
                if (value == null) {
                    if (idx == args.size() - 1) {
                        throw new IllegalArgumentException(
                                String.format("Missing argument for option '%s'.", arg));
                    }
                    idx++;
                    value = args.get(idx);
                }
                if (!handle(arg, value)) {
                    break;
                }
                continue;
            }

            // 在switch中查找[若未找到, 由handleUnknown处理]
            name = findCliOption(arg, switches);
            if (name != null) {
                if (!handle(name, null)) {
                    break;
                }
                continue;
            }

            if (!handleUnknown(arg)) {
                break;
            }
        }

        if (idx < args.size()) {
            idx++;
        }
        handleExtraArgs(args.subList(idx, args.size()));
    }

    private String findCliOption(String name, String[][] available) {
        for (String[] candidates : available) {
            for (String candidate : candidates) {
                if (candidate.equals(name)) {
                    return candidates[0];
                }
            }
        }
        return null;
    }

    public void printErrorAndExit(String str) {
        printStream.println("Error: " + str);
        printStream.println("Run with --help for usage help or --verbose for debug output");
        System.exit(1);
    }

    public abstract boolean handle(String opt, String value);

    public abstract boolean handleUnknown(String opt);

    /**
     * {@link #handle(String, String)}或{@link #handleUnknown(String)}处理失败时该方法被调用
     * */
    public abstract void handleExtraArgs(List<String> extra);
}
