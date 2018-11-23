package com.nameof.zookeeper.util.queue;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

/**
 * @author chengpan
 */
public abstract class AbstractZkBlockingQueue extends AbstractZkQueue implements BlockingQueue<Object> {

    public AbstractZkBlockingQueue(String queueName, String connectString, Serializer serializer) throws IOException, InterruptedException, KeeperException {
        super(queueName, connectString, serializer);
    }
}
