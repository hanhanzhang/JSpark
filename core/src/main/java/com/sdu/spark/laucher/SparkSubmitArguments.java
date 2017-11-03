package com.sdu.spark.laucher;

import com.google.common.collect.Maps;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.sdu.spark.laucher.SparkSubmitAction.KILL;
import static com.sdu.spark.laucher.SparkSubmitAction.REQUEST_STATUS;
import static com.sdu.spark.utils.Utils.resolveURIs;

/**
 * @author hanhan.zhang
 * */
public class SparkSubmitArguments extends SparkSubmitArgumentsParser {

    // Spark Master地址
    public String master;
    // 部署类型
    public String deployMode;
    
    // Executor JVM内存
    public String executorMemory;
    // Executor分配CPU核数
    public String executorCores;
    // 分配总CPU核数
    public String totalExecutorCores;
    public String propertiesFile;
    
    // Spark Application Driver分配JVM内存
    public String driverMemory;
    public String driverExtraClassPath;
    public String driverExtraLibraryPath;
    public String driverExtraJavaOptions;
    
    public String queue;
    public String numExecutors;
    public String files;
    public String archives;

    // 程序运行入口
    public String mainClass;
    public String primaryResource;
    public String name;
    public List<String> childArgs;
    public String jars;
    public String packages;
    public String repositories;
    public String ivyRepoPath;
    public String packagesExclusions;
    public boolean verbose = false;
    public boolean isPython = false;
    public String pyFiles;
    public boolean isR = false;
    public SparkSubmitAction action;
    public Map<String, String> sparkProperties = Maps.newHashMap();
    public String proxyUser = null;
    public String principal = null;
    public String keytab = null;

    // Standalone cluster mode only
    public boolean supervise = false;
    public String driverCores;
    public String submissionToKill;
    public String submissionToRequestStatusFor;
    public boolean useRest= true;

    public SparkSubmitArguments(String[] args) {
        parse(Arrays.asList(args));
    }

    @Override
    public boolean handle(String opt, String value) {
        switch (opt) {
            case NAME:
                name = value;
                return true;
            
            case MASTER:
                master = value;
                return true;
            
            case CLASS:
                mainClass = value;
                return true;
            
            case DEPLOY_MODE:
                if (!value.equals("client") && !value.equals("cluster")) {
                    printErrorAndExit("--deploy-mode must be either \"client\" or \"cluster\"");
                }
                deployMode = value;
                return true;
            
            case NUM_EXECUTORS:
                numExecutors = value;
                return true;

            case TOTAL_EXECUTOR_CORES:
                totalExecutorCores = value;
                return true;
            
            case EXECUTOR_CORES:
                executorCores = value;
                return true;

            case EXECUTOR_MEMORY :
                executorMemory = value;
                return true;

            case DRIVER_MEMORY :
                driverMemory = value;
                return true;

            case DRIVER_CORES :
                driverCores = value;
                return true;

            case DRIVER_CLASS_PATH :
                driverExtraClassPath = value;
                return true;

            case DRIVER_JAVA_OPTIONS :
                driverExtraJavaOptions = value;
                return true;

            case DRIVER_LIBRARY_PATH :
                driverExtraLibraryPath = value;
                return true;

            case PROPERTIES_FILE :
                propertiesFile = value;
                return true;

            case KILL_SUBMISSION :
                submissionToKill = value;
                if (action != null) {
                    printErrorAndExit("Action cannot be both " + action + " and " + KILL + ".");
                }
                action = KILL;
                return true;

            case STATUS :
                submissionToRequestStatusFor = value;
                if (action != null) {
                    printErrorAndExit("Action cannot be both " + action + " and " + REQUEST_STATUS + ".");
                }
                action = REQUEST_STATUS;
                return true;

            case SUPERVISE :
                supervise = true;
                return true;

            case QUEUE :
                queue = value;
                return true;

            case FILES :
                files = resolveURIs(value);
                return true;

            case PY_FILES :
                pyFiles = resolveURIs(value);
                return true;

            case ARCHIVES :
                archives = resolveURIs(value);
                return true;

            case JARS :
                jars = resolveURIs(value);
                return true;

            case PACKAGES :
                packages = value;
                return true;

            case PACKAGES_EXCLUDE :
                packagesExclusions = value;
                return true;

            case REPOSITORIES :
                repositories = value;
                return true;

            case CONF :
                String[] pair = value.split("=");
                if (pair.length == 2) {
                    sparkProperties.put(pair[0], pair[1]);
                }
                return true;

            case PROXY_USER :
                proxyUser = value;
                return true;

            case PRINCIPAL :
                principal = value;
                return true;

            case KEYTAB :
                keytab = value;
                return true;

            case HELP :
                printUsageAndExit(0, null);
                return true;

            case VERBOSE :
                verbose = true;
                return true;

            case VERSION :
                return true;

            case USAGE_ERROR :
                return true;

            default:
                throw new IllegalArgumentException("Unexpected argument " + opt + ".");
        }
    }

    @Override
    public boolean handleUnknown(String opt) {
        return false;
    }

    @Override
    public void handleExtraArgs(List<String> extra) {

    }

    private void printUsageAndExit(int exitCode, Object unknownParam) {
        PrintStream outStream = printStream;
        if (unknownParam != null) {
            outStream.println("Unknown/unsupported param " + unknownParam);
        }
        String command = System.getenv().getOrDefault("_SPARK_CMD_USAGE",
                "Usage: spark-submit [options] <app jar | python file> [app arguments]\n" +
                        "Usage: spark-submit --kill [submission ID] --master [spark://...]\n" +
                        "Usage: spark-submit --status [submission ID] --master [spark://...]\n" +
                        "Usage: spark-submit run-example [options] example-class [example args]");
        outStream.println(command);

        outStream.println("\"\n" +
                "Options:\n" +
                "  --master MASTER_URL         spark://host:port, mesos://host:port, yarn, or local.\n" +
                "  --deploy-mode DEPLOY_MODE   Whether to launch the driver program locally (\"client\") or\n" +
                "                              on one of the worker machines inside the cluster (\"cluster\")\n" +
                "                              (Default: client).\n" +
                "  --class CLASS_NAME          Your application's main class (for Java / Scala apps).\n" +
                "  --name NAME                 A name of your application.\n" +
                "  --jars JARS                 Comma-separated list of local jars to include on the driver\n" +
                "                              and executor classpaths.\n" +
                "  --packages                  Comma-separated list of maven coordinates of jars to include\n" +
                "                              on the driver and executor classpaths. Will search the local\n" +
                "                              maven repo, then maven central and any additional remote\n" +
                "                              repositories given by --repositories. The format for the\n" +
                "                              coordinates should be groupId:artifactId:version.\n" +
                "  --exclude-packages          Comma-separated list of groupId:artifactId, to exclude while\n" +
                "                              resolving the dependencies provided in --packages to avoid\n" +
                "                              dependency conflicts.\n" +
                "  --repositories              Comma-separated list of additional remote repositories to\n" +
                "                              search for the maven coordinates given with --packages.\n" +
                "  --py-files PY_FILES         Comma-separated list of .zip, .egg, or .py files to place\n" +
                "                              on the PYTHONPATH for Python apps.\n" +
                "  --files FILES               Comma-separated list of files to be placed in the working\n" +
                "                              directory of each executor. File paths of these files\n" +
                "                              in executors can be accessed via SparkFiles.get(fileName).\n" +
                "\n" +
                "  --conf PROP=VALUE           Arbitrary Spark configuration property.\n" +
                "  --properties-file FILE      Path to a file from which to load extra properties. If not\n" +
                "                              specified, this will look for conf/spark-defaults.conf.\n" +
                "\n" +
                "  --driver-memory MEM         Memory for driver (e.g. 1000M, 2G) (Default: ${mem_mb}M).\n" +
                "  --driver-java-options       Extra Java options to pass to the driver.\n" +
                "  --driver-library-path       Extra library path entries to pass to the driver.\n" +
                "  --driver-class-path         Extra class path entries to pass to the driver. Note that\n" +
                "                              jars added with --jars are automatically included in the\n" +
                "                              classpath.\n" +
                "\n" +
                "  --executor-memory MEM       Memory per executor (e.g. 1000M, 2G) (Default: 1G).\n" +
                "\n" +
                "  --proxy-user NAME           User to impersonate when submitting the application.\n" +
                "                              This argument does not work with --principal / --keytab.\n" +
                "\n" +
                "  --help, -h                  Show this help message and exit.\n" +
                "  --verbose, -v               Print additional debug combiner.\n" +
                "  --version,                  Print the version of current Spark.\n" +
                "\n" +
                " Cluster deploy mode only:\n" +
                "  --driver-cores NUM          Number of cores used by the driver, only in cluster mode\n" +
                "                              (Default: 1).\n" +
                "\n" +
                " Spark standalone or Mesos with cluster deploy mode only:\n" +
                "  --supervise                 If given, restarts the driver on failure.\n" +
                "  --kill SUBMISSION_ID        If given, kills the driver specified.\n" +
                "  --status SUBMISSION_ID      If given, requests the status of the driver specified.\n" +
                "\n" +
                " Spark standalone and Mesos only:\n" +
                "  --total-executor-cores NUM  Total cores for all executors.\n" +
                "\n" +
                " Spark standalone and YARN only:\n" +
                "  --executor-cores NUM        Number of cores per executor. (Default: 1 in YARN mode,\n" +
                "                              or all available cores on the worker in standalone mode)\n" +
                "\n" +
                " YARN-only:\n" +
                "  --queue QUEUE_NAME          The YARN queue to submit to (Default: \"default\").\n" +
                "  --num-executors NUM         Number of executors to launch (Default: 2).\n" +
                "                              If dynamic allocation is enabled, the initialCollection number of\n" +
                "                              executors will be at least NUM.\n" +
                "  --archives ARCHIVES         Comma separated list of archives to be extracted into the\n" +
                "                              working directory of each executor.\n" +
                "  --principal PRINCIPAL       Principal to be used to login to KDC, while running on\n" +
                "                              secure HDFS.\n" +
                "  --keytab KEYTAB             The full path to the file that contains the keytab for the\n" +
                "                              principal specified above. This keytab will be copied to\n" +
                "                              the node running the Application Master via the Secure\n" +
                "                              Distributed Cache, for renewing the login tickets and the\n" +
                "                              delegation tokens periodically.\n" +
                "      \"");
    }

}
