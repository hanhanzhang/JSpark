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
    private static Pattern LOCAL_N_REGEX = Pattern.compile("local\\[([0-9]+|\\*)\\]");
    // Regular expression for local[N, maxRetries], used in tests with failing tasks
    private static Pattern LOCAL_N_FAILURES_REGEX = Pattern.compile("local\\[([0-9]+|\\*)\\s*,\\s*([0-9]+)\\]");
    // Regular expression for simulating a Spark cluster of [N, cores, memory] locally
    private static Pattern LOCAL_CLUSTER_REGEX = Pattern.compile("local-cluster\\[\\s*([0-9]+)\\s*,\\s*([0-9]+)\\s*,\\s*([0-9]+)\\s*]");
    // Regular expression for connecting to Spark deploy clusters
    private static Pattern SPARK_REGEX = Pattern.compile("spark://(.*)");

    public static boolean LOCAL_N_REGEX(String master) {
        Matcher m = LOCAL_N_REGEX.matcher(master);
        return m.matches();
    }

    public static String LOCAL_N_REGEX_THREAD(String master) {
        Matcher m = LOCAL_N_REGEX.matcher(master);
        if (m.find()) {
            return m.group(1);
        }
        return "*";
    }

    public static boolean LOCAL_N_FAILURES_REGEX(String master) {
        Matcher m = LOCAL_N_FAILURES_REGEX.matcher(master);
        return m.matches();
    }

    public static String[] LOCAL_N_FAILURES_REGEX_R(String master) {
        Matcher m = LOCAL_N_FAILURES_REGEX.matcher(master);
        if (m.find()) {
            String[] r = new String[2];
            r[0] = m.group(1);
            r[1] = m.group(2);
            return r;
        }

        return new String[] {"*", "1"};
    }

    public static boolean LOCAL_CLUSTER_REGEX(String master) {
        Matcher m = LOCAL_CLUSTER_REGEX.matcher(master);
        return m.matches();
    }

    public static boolean SPARK_REGEX(String master) {
        Matcher m = SPARK_REGEX.matcher(master);
        return m.matches();
    }

    public static void main(String[] args) {
        System.out.println(LOCAL_N_REGEX("local[1]"));
        System.out.println(LOCAL_N_REGEX_THREAD("local[2]"));
        System.out.println(LOCAL_N_FAILURES_REGEX("local[1, 3]"));
        String[] r = LOCAL_N_FAILURES_REGEX_R("local[1, 3]");
        System.out.println(StringUtils.join(r, ','));
        System.out.println(LOCAL_CLUSTER_REGEX("local-cluster[10, 1, 1024]"));
        System.out.println(SPARK_REGEX("spark://test"));
    }
}
