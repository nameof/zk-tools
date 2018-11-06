package com.nameof.zookeeper.util.queue;

public interface Serializer {
    byte[] serialize(Object obj);
    Object deserialize(byte[] bytes);
}
