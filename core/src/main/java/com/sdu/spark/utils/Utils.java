package com.sdu.spark.utils;

import com.google.common.collect.ImmutableMap;
import com.sdu.spark.rpc.SparkConf;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author hanhan.zhang
 * */
public class Utils {

    private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class);

    private static boolean isWindows = SystemUtils.IS_OS_WINDOWS;
    private static boolean isMac = SystemUtils.IS_OS_MAC;

    private static final ImmutableMap<String, TimeUnit> timeSuffixes = ImmutableMap.<String, TimeUnit> builder()
                                                                                    .put("us", TimeUnit.MICROSECONDS)
                                                                                    .put("ms", TimeUnit.MILLISECONDS)
                                                                                    .put("s", TimeUnit.SECONDS)
                                                                                    .put("m", TimeUnit.MINUTES)
                                                                                    .put("min", TimeUnit.MINUTES)
                                                                                    .put("h", TimeUnit.HOURS)
                                                                                    .put("d", TimeUnit.DAYS)
                                                                                    .build();

    public static <T> T getFutureResult(Future<?> future) {
        try {
            return (T) future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
            LOGGER.error("future task interrupted exception", e);
            throw new RuntimeException("future task interrupted exception", e);
        } catch (ExecutionException e) {
            LOGGER.error("future task execute exception", e);
            e.printStackTrace();
            throw new RuntimeException("future task execute exception", e);
        }
    }


    public static int convertStringToInt(String text) {
        return Integer.parseInt(text);
    }

    public static String libraryPathEnvName() {
        if (isWindows) {
            return "PATH";
        } else if (isMac) {
            return "DYLD_LIBRARY_PATH";
        } else {
            return "LD_LIBRARY_PATH";
        }
    }

    public static long copyStream(InputStream input, OutputStream out, boolean transferToEnabled) throws IOException {
        long count = 0;
        try {
            if (input instanceof FileInputStream && out instanceof FileOutputStream && transferToEnabled) {
                FileChannel inputChannel = ((FileInputStream) input).getChannel();
                FileChannel outputChanel = ((FileOutputStream) out).getChannel();
                count = inputChannel.size();
                copyFileStreamNIO(inputChannel, outputChanel, 0, count);
            } else {
                byte[] buf = new byte[8192];
                int n = 0;
                while (n != -1) {
                    n = input.read(buf);
                    if (n != -1) {
                        out.write(buf, 0, n);
                        count += n;
                    }
                }
            }
        } finally {
            if (input != null) {
                input.close();
            }
            if (out != null) {
                out.close();
            }
        }
        return count;
    }

    public static void writeByteBuffer(ByteBuffer bb, DataOutput out) throws IOException {
        if (bb.hasArray()) {
            out.write(bb.array(), bb.arrayOffset() + bb.position(), bb.remaining());
        } else {
            int originalPosition = bb.position();
            byte[] remainBytes = new byte[bb.remaining()];
            bb.get(remainBytes);
            out.write(remainBytes);
            bb.position(originalPosition);
        }
    }

    private static void copyFileStreamNIO(FileChannel input, FileChannel out, int startPosition, long bytesToCopy) throws IOException {
        long initialPos = out.position();
        long count = 0L;
        while (count < bytesToCopy) {
            count += input.transferTo(count + startPosition, bytesToCopy - count, out);
        }
        assert count == bytesToCopy :
                String.format("需要复制%s字节数据, 实际复制%s字节数据", bytesToCopy, count);
        long finalPos = out.position();
        long expectedPos = initialPos + bytesToCopy;
        assert finalPos == expectedPos :
                String.format("Current position %s do not equal to expected position %s", finalPos, expectedPos);
    }

    public static String resolveURIs(String paths) {
        if (paths == null || paths.trim().isEmpty()) {
            return "";
        } else {
            List<URI> uriList = Arrays.stream(paths.split(",")).filter(p -> !p.isEmpty()).map(Utils::resolveURI).collect(Collectors.toList());
            return StringUtils.join(uriList, ",");
        }
    }

    public static URI resolveURI(String path) {
        try {
            URI uri = new URI(path);
            if (uri.getScheme() != null) {
                return uri;
            }
            if (uri.getFragment() != null) {
                URI absoluteURI = new File(uri.getPath()).getAbsoluteFile().toURI();
                return new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
                        uri.getFragment());
            }
        } catch (Exception e) {
            // ignore
        }
        return new File(path).getAbsoluteFile().toURI();
    }

    public static long timeStringAs(String str, TimeUnit unit) {
        String lower = str.toLowerCase(Locale.ROOT).trim();

        try {
            Matcher m = Pattern.compile("(-?[0-9]+)([a-z]+)?").matcher(lower);
            if (!m.matches()) {
                throw new NumberFormatException("Failed to parse time string: " + str);
            }

            long val = Long.parseLong(m.group(1));
            String suffix = m.group(2);

            // Check for invalid suffixes
            if (suffix != null && !timeSuffixes.containsKey(suffix)) {
                throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
            }

            // If suffix is valid use that, otherwise none was provided and use the default passed
            return unit.convert(val, suffix != null ? timeSuffixes.get(suffix) : unit);
        } catch (NumberFormatException e) {
            String timeError = "Time must be specified as seconds (s), " +
                    "milliseconds (ms), microseconds (us), minutes (m or min), hour (h), or day (d). " +
                    "E.g. 50s, 100ms, or 250us.";

            throw new NumberFormatException(timeError + "\n" + e.getMessage());
        }
    }

    public static long computeTotalGcTime() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream()
                                    .map(GarbageCollectorMXBean::getCollectionTime).count();
    }

    public static Class<?> classForName(String className){
        try {
            return Class.forName(className, true, Thread.currentThread().getContextClassLoader());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

    }

    public static boolean isLocalMaster(SparkConf conf) {
        String master = conf.get("spark.master", "");
        return master.equals("local") || master.startsWith("local[");
    }

    public static String localHostName() {
        try {
            return findLocalInetAddress().getHostAddress();
        } catch (Exception e) {
            return "localhost";
        }
    }

    private static InetAddress findLocalInetAddress() throws Exception {
        String defaultIpOverride = System.getenv("SPARK_LOCAL_IP");
        if (defaultIpOverride != null) {
            return InetAddress.getByName(defaultIpOverride);
        } else {
           return InetAddress.getLocalHost();
        }
    }

    public static String bytesToString(long size) {
        return bytesToString(BigInteger.valueOf(size));
    }

    private static String bytesToString(BigInteger size) {
        long EB = 1L << 60;
        long PB = 1L << 50;
        long TB = 1L << 40;
        long GB = 1L << 30;
        long MB = 1L << 20;
        long KB = 1L << 10;

        if (size.compareTo(BigInteger.valueOf(1L << 11 * EB)) > 0) {
            // The number is too large, show it in scientific notation.
            return new BigDecimal(size, new MathContext(3, RoundingMode.HALF_UP)).toString() + " B";
        } else {
            if (size.compareTo(BigInteger.valueOf(2 * EB)) > 0) {
                String value = new BigDecimal(size).divide(new BigDecimal(EB), 1, RoundingMode.HALF_UP).toString();
                return String.format("%s.1f %s", value, "EB");
            } else if (size.compareTo(BigInteger.valueOf(2 * PB)) > 0) {
                String value = new BigDecimal(size).divide(new BigDecimal(PB), 1, RoundingMode.HALF_UP).toString();
                return String.format("%s.1f %s", value, "PB");
            } else if (size.compareTo(BigInteger.valueOf(2 * TB)) > 0) {
                String value = new BigDecimal(size).divide(new BigDecimal(TB), 1, RoundingMode.HALF_UP).toString();
                return String.format("%s.1f %s", value, "TB");
            } else if (size.compareTo(BigInteger.valueOf(2 * GB)) > 0) {
                String value = new BigDecimal(size).divide(new BigDecimal(GB), 1, RoundingMode.HALF_UP).toString();
                return String.format("%s.1f %s", value, "GB");
            } else if (size.compareTo(BigInteger.valueOf(2 * MB)) > 0) {
                String value = new BigDecimal(size).divide(new BigDecimal(MB), 1, RoundingMode.HALF_UP).toString();
                return String.format("%s.1f %s", value, "MB");
            } else if (size.compareTo(BigInteger.valueOf(2 * KB)) > 0) {
                String value = new BigDecimal(size).divide(new BigDecimal(KB), 1, RoundingMode.HALF_UP).toString();
                return String.format("%s.1f %s", value, "KB");
            } else {
                return String.format("%s.1f %s", size.toString(), "B");
            }
        }
    }


}
