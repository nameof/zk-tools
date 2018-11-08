package com.nameof.zookeeper.util.queue;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.nameof.zookeeper.util.utils.ZkUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 无界阻塞队列
 * @author chengpan
 */
public class ZkBlockingQueue extends BaseZkBlockingQueue {

    public ZkBlockingQueue(String queueName, String connectString, Serializer serializer) throws IOException, InterruptedException, KeeperException {
        super(queueName, connectString, serializer);
    }

    /**
     * never blocking
     * @param o
     * @throws InterruptedException
     */
    @Override
    public void put(Object o) throws InterruptedException {
        checkState();
        try {
            ZkUtils.crecatePersistSeq(zk, queuePath + "/", serializer.serialize(o));
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @see #put(Object)
     * @param o
     * @param timeout
     * @param unit
     * @return
     * @throws InterruptedException
     */
    @Override
    public boolean offer(Object o, long timeout, TimeUnit unit) throws InterruptedException {
        this.put(o);
        return true;
    }

    @Override
    public Object take() throws InterruptedException {
        Object o = null;
        while ((o = poll()) == null) {
            CountDownLatch cdl = new CountDownLatch(1);
            EventLatchWatcher elw = new EventLatchWatcher(Watcher.Event.EventType.NodeChildrenChanged, cdl);
            try {
                zk.getChildren(queuePath, elw);
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            }
            cdl.await();
        }
        return o;
    }

    @Override
    public Object poll(long timeout, TimeUnit unit) throws InterruptedException {
        checkState();
        long total = unit.toMillis(timeout);
        long start = System.currentTimeMillis();
        long end = System.currentTimeMillis();
        long waitMillis = total - (end - start);
        Object o = null;
        while ((o = poll()) == null && waitMillis > 0) {
            CountDownLatch cdl = new CountDownLatch(1);
            EventLatchWatcher elw = new EventLatchWatcher(Watcher.Event.EventType.NodeChildrenChanged, cdl);
            try {
                zk.getChildren(queuePath, elw);
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            }
            cdl.await(waitMillis, TimeUnit.MILLISECONDS);
            end = System.currentTimeMillis();
            waitMillis = total - (end - start);
        }
        return o;
    }

    @Override
    public int drainTo(Collection<? super Object> c) {
        checkState();
        try {
            List<Object> all = ZkUtils.takeAllChildrenData(zk, queuePath, serializer);
            c.addAll(all);
            return all.size();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int drainTo(Collection<? super Object> c, int maxElements) {
        checkState();
        try {
            List<Object> all = ZkUtils.takeAllChildrenData(zk, queuePath, serializer, maxElements);
            c.addAll(all);
            return all.size();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class EventLatchWatcher implements Watcher {

        private final Event.EventType type;
        private final CountDownLatch cdl;

        public EventLatchWatcher(Event.EventType type, CountDownLatch cdl) {
            this.type = type;
            this.cdl = cdl;
        }

        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == type)
                cdl.countDown();
        }
    }
}
