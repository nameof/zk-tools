package com.nameof.zookeeper.tools.queue;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * 无界非阻塞队列
 */
public class ZkQueue extends AbstractZkQueue {

    public ZkQueue(String queueName, String connectString, Serializer serializer) throws IOException, InterruptedException, KeeperException {
        super(queueName, connectString, serializer);
    }
}
