package com.sdu.spark.network.utils;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author hanhan.zhang
 * */
public class JavaUtils {

    private static final ImmutableMap<String, TimeUnit> timeSuffixes = ImmutableMap.<String, TimeUnit>builder()
                                                                                    .put("us", TimeUnit.MICROSECONDS)
                                                                                    .put("ms", TimeUnit.MILLISECONDS)
                                                                                    .put("s", TimeUnit.SECONDS)
                                                                                    .put("m", TimeUnit.MINUTES)
                                                                                    .put("min", TimeUnit.MINUTES)
                                                                                    .put("h", TimeUnit.HOURS)
                                                                                    .put("d", TimeUnit.DAYS)
                                                                                    .build();

    private static final ImmutableMap<String, ByteUnit> byteSuffixes = ImmutableMap.<String, ByteUnit>builder()
                                                                                    .put("b", ByteUnit.BYTE)
                                                                                    .put("k", ByteUnit.KiB)
                                                                                    .put("kb", ByteUnit.KiB)
                                                                                    .put("m", ByteUnit.MiB)
                                                                                    .put("mb", ByteUnit.MiB)
                                                                                    .put("g", ByteUnit.GiB)
                                                                                    .put("gb", ByteUnit.GiB)
                                                                                    .put("t", ByteUnit.TiB)
                                                                                    .put("tb", ByteUnit.TiB)
                                                                                    .put("p", ByteUnit.PiB)
                                                                                    .put("pb", ByteUnit.PiB)
                                                                                    .build();

    public static void closeQuietly(Closeable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
            // ignore
        }
    }

    public static byte[] bufferToArray(ByteBuffer buffer) {
        if (buffer.hasArray() && buffer.arrayOffset() == 0 &&
                buffer.array().length == buffer.remaining()) {
            return buffer.array();
        } else {
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        }
    }


    public static long byteStringAsBytes(String str) {
        return byteStringAs(str, ByteUnit.BYTE);
    }

    public static long byteStringAs(String str, ByteUnit unit) {
        String lower = str.toLowerCase(Locale.ROOT).trim();

        try {
            Matcher m = Pattern.compile("([0-9]+)([a-z]+)?").matcher(lower);
            Matcher fractionMatcher = Pattern.compile("([0-9]+\\.[0-9]+)([a-z]+)?").matcher(lower);

            if (m.matches()) {
                long val = Long.parseLong(m.group(1));
                String suffix = m.group(2);

                // Check for invalid suffixes
                if (suffix != null && !byteSuffixes.containsKey(suffix)) {
                    throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
                }

                // If suffix is valid use that, otherwise none was provided and use the default passed
                return unit.convertFrom(val, suffix != null ? byteSuffixes.get(suffix) : unit);
            } else if (fractionMatcher.matches()) {
                throw new NumberFormatException("Fractional values are not supported. Input was: "
                        + fractionMatcher.group(1));
            } else {
                throw new NumberFormatException("Failed to parse byte string: " + str);
            }

        } catch (NumberFormatException e) {
            String byteError = "Size must be specified as bytes (b), " +
                    "kibibytes (k), mebibytes (m), gibibytes (g), tebibytes (t), or pebibytes(p). " +
                    "E.g. 50b, 100k, or 250m.";

            throw new NumberFormatException(byteError + "\n" + e.getMessage());
        }
    }

    public static long timeStringAsSec(String str) {
        return timeStringAs(str, TimeUnit.SECONDS);
    }

    public static long timeStringAs(String str, TimeUnit unit) {
        String lower = str.toLowerCase(Locale.ROOT).trim();

        try {
            Matcher m = Pattern.compile("(-?[0-9]+)([a-z]+)?").matcher(lower);
            if (!m.matches()) {
                throw new NumberFormatException("Failed to parse time string: " + str);
            }

            long val = NumberUtils.toLong(m.group(1));
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

    public static void readFully(ReadableByteChannel channel, ByteBuffer dst) throws IOException {
        int expected = dst.remaining();
        while (dst.hasRemaining()) {
            if (channel.read(dst) < 0) {
                throw new EOFException(String.format("Not enough bytes in channel (expected %d).", expected));
            }
        }
    }
}
