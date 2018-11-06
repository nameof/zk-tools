package com.nameof.zookeeper.util.queue;

public interface Serializer {
    /**
     * @param obj 可为null
     * @return 当输入参数为null时，返回null
     */
    byte[] serialize(Object obj);

    /**
     * @param bytes 可为null
     * @return 当输入参数为null时，返回null
     */
    Object deserialize(byte[] bytes);
}
