package com.sdu.spark.serializer;

/**
 * TODO: KryoSerializer
 *
 * @author hanhan.zhang
 * */
public interface Serializer {

    SerializerInstance newInstance();

    default boolean supportsRelocationOfSerializedObjects() {
        return false;
    }


}
