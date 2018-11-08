package com.nameof.zookeeper.util.queue;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
/**
 * @author chengpan
 */
public abstract class BaseZkBlockingQueue extends BaseZkQueue implements BlockingQueue<Object> {

    public BaseZkBlockingQueue(String queueName, String connectString, Serializer serializer) throws IOException, InterruptedException, KeeperException {
        super(queueName, connectString, serializer);
    }

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }
}
