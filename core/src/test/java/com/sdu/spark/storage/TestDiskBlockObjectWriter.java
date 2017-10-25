package com.sdu.spark.storage;

import com.sdu.spark.SparkTestUnit;
import com.sdu.spark.serializer.JavaSerializer;
import com.sdu.spark.serializer.JavaSerializerInstance;
import com.sdu.spark.serializer.Serializer;
import com.sdu.spark.serializer.SerializerManager;
import com.sdu.spark.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * @author hanhan.zhang
 * */
public class TestDiskBlockObjectWriter extends SparkTestUnit {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestDiskBlockObjectWriter.class);

    private SerializerManager serializerManager;
    private Serializer defaultSerializer;

    private File tempDir;

    @Override
    public void beforeEach() {
        try {
            this.tempDir = Utils.createTempDir();
        } catch (IOException e) {
            return;
        }

        this.defaultSerializer = new JavaSerializer(conf);
        this.serializerManager = new SerializerManager(defaultSerializer, conf);
    }

    private DiskBlockObjectWriter createWriter() {
        File file = new File(tempDir, "testfile");
        return new DiskBlockObjectWriter(
                file,
                serializerManager,
                new JavaSerializer(conf).newInstance(),
                1024,
                true
        );
    }

    @Test
    public void testCommitAndClose() throws IOException {
        DiskBlockObjectWriter writer = createWriter();
        FileSegment fileSegment = writer.commitAndGet();
        writer.close();
        assert fileSegment.length == 0;
    }

    @Test
    public void testWriteAndCommit() throws IOException {
        DiskBlockObjectWriter writer = createWriter();
        for (int i = 0; i < 1000; i++) {
            writer.write(i, i);
        }
        FileSegment segment1 = writer.commitAndGet();
        LOGGER.info("seg : offset = {}, length = {}", segment1.offset, segment1.length);

        for (int i = 0; i < 1000; ++i) {
            writer.write(i, i);
        }
        FileSegment segment2 = writer.commitAndGet();
        LOGGER.info("seg : offset = {}, length = {}", segment2.offset, segment2.length);

        writer.close();
        assert segment2.offset == segment1.length;
    }

    @Override
    public void afterEach() {
        try {
            Utils.deleteRecursively(tempDir);
        } catch (IOException e) {
            // ignore
        }
    }
}
