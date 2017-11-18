package com.sdu.spark.scheduler;

import com.google.common.collect.Maps;
import com.sdu.spark.SparkException;
import com.sdu.spark.utils.ByteBufferInputStream;
import com.sdu.spark.utils.ByteBufferOutputStream;
import com.sdu.spark.utils.Utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static com.sdu.spark.utils.Utils.writeByteBuffer;

/**
 * @author hanhan.zhang
 * */
public class TaskDescription {

    public long taskId;
    public int attemptNumber;
    public String executorId;
    public String name;
    public int index;    // Index withIn this task's TaskSet
    public Map<String, Long> addedFiles;
    public Map<String, Long> addedJars;
    public Properties properties;
    public ByteBuffer serializedTask;

    public TaskDescription(long taskId, int attemptNumber, String executorId, String name, int index,
                           Map<String, Long> addedFiles, Map<String, Long> addedJars, Properties properties,
                           ByteBuffer serializedTask) {
        this.taskId = taskId;
        this.attemptNumber = attemptNumber;
        this.executorId = executorId;
        this.name = name;
        this.index = index;
        this.addedFiles = addedFiles;
        this.addedJars = addedJars;
        this.properties = properties;
        this.serializedTask = serializedTask;
    }

    @Override
    public String toString() {
        return String.format("TaskDescription(TID=%d, index=%d)", taskId, index);
    }

    private static void serializeStringLongMap(Map<String, Long> map, DataOutputStream dataOut) throws IOException {
        dataOut.writeInt(map.size());
        for (Map.Entry<String, Long> entry : map.entrySet()) {
            dataOut.writeUTF(entry.getKey());
            dataOut.writeLong(entry.getValue());
        }
    }

    private static Map<String, Long> deserializeStringLongMap(DataInputStream dataIn) throws IOException {
        Map<String, Long> map = Maps.newHashMap();
        int mapSize = dataIn.readInt();
        for (int i = 0; i < mapSize; ++i) {
            String key = dataIn.readUTF();
            long value = dataIn.readLong();
            map.put(key, value);
        }
        return map;
    }

    public static ByteBuffer encode(TaskDescription taskDescription) throws IOException {
        ByteBufferOutputStream bytesOut = new ByteBufferOutputStream(4096);
        DataOutputStream dataOut = new DataOutputStream(bytesOut);
        try {
            dataOut.writeLong(taskDescription.taskId);
            dataOut.writeInt(taskDescription.attemptNumber);
            dataOut.writeUTF(taskDescription.executorId);
            dataOut.writeUTF(taskDescription.name);
            dataOut.writeInt(taskDescription.index);

            // write files
            serializeStringLongMap(taskDescription.addedFiles, dataOut);

            // write jars
            serializeStringLongMap(taskDescription.addedJars, dataOut);

            // write properties
            dataOut.writeInt(taskDescription.properties.size());
            Iterator<String> iterator = taskDescription.properties.stringPropertyNames().iterator();
            while (iterator.hasNext()) {
                String key = iterator.next();
                String value = taskDescription.properties.getProperty(key);
                byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                dataOut.writeUTF(key);
                dataOut.writeInt(bytes.length);
                dataOut.write(bytes);
            }

            writeByteBuffer(taskDescription.serializedTask, dataOut);
            return bytesOut.toByteBuffer();
        } finally {
            dataOut.close();
            bytesOut.close();
        }
    }

    public static TaskDescription decode(ByteBuffer byteBuffer) {
        try {
            DataInputStream dataIn = new DataInputStream(new ByteBufferInputStream(byteBuffer));

            long taskId = dataIn.readLong();
            int attemptNumber = dataIn.readInt();
            String executorId = dataIn.readUTF();
            String name = dataIn.readUTF();
            int index = dataIn.readInt();

            // Read files.
            Map<String, Long> taskFiles = deserializeStringLongMap(dataIn);

            // Read jars.
            Map<String, Long> taskJars = deserializeStringLongMap(dataIn);

            // Read properties.
            Properties properties = new Properties();
            int numProperties = dataIn.readInt();
            for (int i = 0; i < numProperties; ++i) {
                String key = dataIn.readUTF();
                int valueLength = dataIn.readInt();
                byte[] valueBytes = new byte[valueLength];
                dataIn.readFully(valueBytes);
                properties.setProperty(key, new String(valueBytes, StandardCharsets.UTF_8));
            }

            // Create a sub-buffer for the serialized task into its own buffer (to be deserialized later).
            ByteBuffer serializedTask = byteBuffer.slice();

            return new TaskDescription(taskId, attemptNumber, executorId, name, index, taskFiles, taskJars,
                    properties, serializedTask);
        } catch (IOException e) {
            throw new SparkException("deserializer task failure", e);
        }
    }
}
