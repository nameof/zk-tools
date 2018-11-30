package com.nameof.zookeeper.tools.queue;

/**
 * @author chengpan
 */
public interface Serializer {
    /**
     * @param obj 不允许null
     * @return
     */
    byte[] serialize(Object obj);

    /**
     * @param bytes 不允许null
     * @return
     */
    Object deserialize(byte[] bytes);
}
