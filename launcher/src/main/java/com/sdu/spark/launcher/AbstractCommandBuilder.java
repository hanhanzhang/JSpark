package com.sdu.spark.launcher;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;
import static com.sdu.spark.launcher.CommandBuilderUtils.firstNonEmpty;
import static com.sdu.spark.launcher.CommandBuilderUtils.join;
import static com.sdu.spark.launcher.CommandBuilderUtils.parseOptionString;


/**
 * @author hanhan.zhang
 * */
public abstract class AbstractCommandBuilder {

    public static final String ENV_SPARK_HOME = "SPARK_HOME";

    /**
     * Java安装包路径
     * */
    String javaHome;
    protected final Map<String, String> childEnv;

    public AbstractCommandBuilder() {
        this.childEnv = Maps.newHashMap();
    }

    public abstract List<String> buildCommand(Map<String, String> env) throws IOException, IllegalArgumentException;

    /**
     * 构建Java运行命令
     * */
    public List<String> buildJavaCommand(String extraClassPath) throws IOException {
        List<String> cmd = Lists.newArrayList();
        String envJavaHome;

        if (javaHome != null) {
            cmd.add(join(File.separator, javaHome, "bin", "java"));
        } else if ((envJavaHome = System.getenv("JAVA_HOME")) != null) {
            cmd.add(join(File.separator, envJavaHome, "bin", "java"));
        } else {
            cmd.add(join(File.separator, System.getProperty("java.home"), "bin", "java"));
        }

        // Load extra JAVA_OPTS from conf/java-opts, if it exists.
        File javaOpts = new File(join(File.separator, getConfDir(), "java-opts"));
        if (javaOpts.isFile()) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(
                    new FileInputStream(javaOpts), StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    addOptionString(cmd, line);
                }
            }
        }

        cmd.add("-cp");
        cmd.add(join(File.pathSeparator, buildClassPath(extraClassPath)));
        return cmd;
    }

    List<String> buildClassPath(String appClassPath) throws IOException {
        Set<String> cp = new LinkedHashSet<>();
        addToClassPath(cp, appClassPath);

        addToClassPath(cp, getConfDir());

        addToClassPath(cp, getenv("HADOOP_CONF_DIR"));
        addToClassPath(cp, getenv("YARN_CONF_DIR"));
        addToClassPath(cp, getenv("SPARK_DIST_CLASSPATH"));
        return new ArrayList<>(cp);
    }

    private void addToClassPath(Set<String> cp, String entries) {
        if (StringUtils.isEmpty(entries)) {
            return;
        }
        String[] split = entries.split(Pattern.quote(File.pathSeparator));
        for (String entry : split) {
            if (StringUtils.isNotEmpty(entry)) {
                if (new File(entry).isDirectory() && !entry.endsWith(File.separator)) {
                    entry += File.separator;
                }
                cp.add(entry);
            }
        }
    }

    void addOptionString(List<String> cmd, String options) {
        if (StringUtils.isBlank(options)) {
            parseOptionString(options).forEach(opt -> cmd.add(opt));
        }
    }


    String getenv(String key) {
        return firstNonEmpty(childEnv.get(key), System.getenv(key));
    }

    String getSparkHome() {
        String path = getenv(ENV_SPARK_HOME);
        checkState(path != null, "Spark home not found; set it explicitly or use the SPARK_HOME environment variable.");
        return path;
    }

    /**
     * Spark安装目录下Conf
     * */
    private String getConfDir() {
        String confDir = getenv("SPARK_CONF_DIR");
        return confDir != null ? confDir : join(File.separator, getSparkHome(), "conf");
    }
}
