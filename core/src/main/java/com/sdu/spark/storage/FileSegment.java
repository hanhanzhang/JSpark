package com.sdu.spark.storage;

import java.io.File;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * @author hanhan.zhang
 * */
public class FileSegment {

    public File file;
    public long offset;
    public long length;

    public FileSegment(File file, long offset, long length) {
        checkArgument(offset >= 0, String.format("File segment offset cannot be negative (got %d)", offset));
        checkArgument(length >= 0, String.format("File segment length cannot be negative (got %d)", length));

        this.file = file;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public String toString() {
        return String.format("(name=%s, offset=%d, length=%d)", file.getName(), offset, length);
    }
}
