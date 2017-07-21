package com.sdu.spark;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author hanhan.zhang
 * */
public class SparkMasterRegex {

    // Regular expression used for local[N] and local[*] master formats
    private static String LOCAL_N_REGEX = "local\\[([0-9]+|\\*)\\]";
    // Regular expression for local[N, maxRetries], used in tests with failing tasks
    private static String LOCAL_N_FAILURES_REGEX = "local\\[([0-9]+|\\*)\\s*,\\s*([0-9]+)\\]";
    // Regular expression for simulating a Spark cluster of [N, cores, memory] locally
    private static String LOCAL_CLUSTER_REGEX = "local-cluster\\[\\s*([0-9]+)\\s*,\\s*([0-9]+)\\s*,\\s*([0-9]+)\\s*]";
    // Regular expression for connecting to Spark deploy clusters
    private static String SPARK_REGEX = "spark://(.*)";

    public static boolean LOCAL_N_REGEX(String master) {
        Pattern p = Pattern.compile(LOCAL_N_REGEX);
        Matcher m = p.matcher(master);
        return m.matches();
    }

    public static int LOCAL_N_REGEX_THREAD(String master) {
        Pattern p = Pattern.compile(LOCAL_N_REGEX);
        Matcher m = p.matcher(master);
        if (m.find()) {
            return NumberUtils.toInt(m.group(1));
        }
        return Runtime.getRuntime().availableProcessors();
    }

    public static boolean LOCAL_N_FAILURES_REGEX(String master) {
        Pattern p = Pattern.compile(LOCAL_N_FAILURES_REGEX);
        Matcher m = p.matcher(master);
        return m.matches();
    }

    public static int[] LOCAL_N_FAILURES_REGEX_R(String master) {
        Pattern p = Pattern.compile(LOCAL_N_FAILURES_REGEX);
        Matcher m = p.matcher(master);
        if (m.find()) {
            int[] r = new int[2];
            r[0] = NumberUtils.toInt(m.group(1));
            r[1] = NumberUtils.toInt(m.group(2));
            return r;
        }

        return new int[] {Runtime.getRuntime().availableProcessors(), 1};
    }

    public static boolean LOCAL_CLUSTER_REGEX(String master) {
        Pattern p = Pattern.compile(LOCAL_CLUSTER_REGEX);
        Matcher m = p.matcher(master);
        return m.matches();
    }

    public static boolean SPARK_REGEX(String master) {
        Pattern p = Pattern.compile(SPARK_REGEX);
        Matcher m = p.matcher(master);
        return m.matches();
    }

    public static void main(String[] args) {
        System.out.println(LOCAL_N_REGEX("local[1]"));
        System.out.println(LOCAL_N_REGEX_THREAD("local[2]"));
        System.out.println(LOCAL_N_FAILURES_REGEX("local[1, 3]"));
        int[] r = LOCAL_N_FAILURES_REGEX_R("local[1, 3]");
        System.out.println(StringUtils.join(r, ','));
        System.out.println(LOCAL_CLUSTER_REGEX("local-cluster[10, 1, 1024]"));
        System.out.println(SPARK_REGEX("spark://test"));
    }
}
