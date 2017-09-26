package com.sdu.spark.network.protocol;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/**
 * @author hanhan.zhang
 * */
public class Encoders {

    public static class Strings {

        public static int encodedLength(String s) {
            return 4 + s.getBytes(StandardCharsets.UTF_8).length;
        }

        public static void encode(ByteBuf buf, String s) {
            byte []bytes = s.getBytes(StandardCharsets.UTF_8);
            buf.writeInt(bytes.length);
            buf.writeBytes(bytes);
        }

        public static String decode(ByteBuf buf) {
            int length = buf.readInt();
            byte []bytes = new byte[length];
            buf.readBytes(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }

    }

    public static class StringArrays {
        public static int encodedLength(String[] strings) {
            int totalLength = 4;
            for (String s : strings) {
                totalLength += Strings.encodedLength(s);
            }
            return totalLength;
        }

        public static void encode(ByteBuf buf, String[] strings) {
            buf.writeInt(strings.length);
            for (String s : strings) {
                Strings.encode(buf, s);
            }
        }

        public static String[] decode(ByteBuf buf) {
            int numStrings = buf.readInt();
            String[] strings = new String[numStrings];
            for (int i = 0; i < strings.length; i ++) {
                strings[i] = Strings.decode(buf);
            }
            return strings;
        }
    }

    public static class ByteArrays {
        public static int encodedLength(byte[] arr) {
            return 4 + arr.length;
        }

        public static void encode(ByteBuf buf, byte[] arr) {
            buf.writeInt(arr.length);
            buf.writeBytes(arr);
        }

        public static byte[] decode(ByteBuf buf) {
            int length = buf.readInt();
            byte[] bytes = new byte[length];
            buf.readBytes(bytes);
            return bytes;
        }
    }

}
