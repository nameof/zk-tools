package com.nameof.zookeeper.util.queue;

import com.google.common.base.Preconditions;
import com.nameof.zookeeper.util.utils.ZkUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;

/**
 * 无界非阻塞队列
 */
public class ZkQueue extends BaseZkQueue {

    public ZkQueue(String queueName, String connectString, Serializer serializer) throws IOException, InterruptedException, KeeperException {
        super(queueName, connectString, serializer);
    }
}
