package com.sdu.spark.storage.memory;

import com.sdu.spark.storage.BlockId;
import com.sdu.spark.storage.StorageLevel;
import com.sdu.spark.utils.ChunkedByteBuffer;
import com.sdu.spark.utils.scala.Either;

import java.util.List;

/**
 * @author hanhan.zhang
 * */
public interface BlockEvictionHandler {

    <T> StorageLevel dropFromMemory(BlockId blockId, Either<List<T>, ChunkedByteBuffer> data);

}
