package com.sdu.spark.storage;

import com.google.common.collect.Lists;
import com.sdu.spark.SparkException;
import com.sdu.spark.SparkTestUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author hanhan.zhang
 * */
public class TestDiskBlockManager extends SparkTestUnit {

    private DiskBlockManager diskBlockManager;

    @Before
    @Override
    public void beforeEach() {
        diskBlockManager = new DiskBlockManager(conf, true);
    }

    @Test
    public void testSingleBlock() throws IOException {
        BlockId.ShuffleBlockId blockId = new BlockId.ShuffleBlockId(1, 1, 0);
        File shuffleFile = diskBlockManager.getFile(blockId);
        writeToFile(shuffleFile, 10);
        assert diskBlockManager.containsBlock(blockId);
    }

    @Test
    public void testMultiBlocks() {
        List<BlockId.ShuffleBlockId> blockIs = Lists.newLinkedList();
        for (int i = 0; i < 100; ++i) {
            blockIs.add(new BlockId.ShuffleBlockId(1, i, 0));
        }
        List<File> shuffleFiles = blockIs.stream().map(diskBlockManager::getFile).collect(Collectors.toList());
        shuffleFiles.forEach(file -> writeToFile(file, 5));

        assert diskBlockManager.getAllBlocks().length == blockIs.size();
    }

    private void writeToFile(File file, int numBytes) {
        try {
            FileWriter writer = new FileWriter(file, true);
            for (int i = 0; i < numBytes; ++i) {
                writer.write(i);
            }
            writer.flush();
            writer.close();
        } catch (IOException e) {
            throw new SparkException(String.format("write to file %s failure", file.getAbsolutePath()), e);
        }
    }

    @After
    public void afterEach() {
        diskBlockManager.stop();
    }
}
