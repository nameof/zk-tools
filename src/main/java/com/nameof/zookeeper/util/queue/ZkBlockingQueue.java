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

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    /**
     * never blocking
     * @param o
     * @throws InterruptedException
     */
    @Override
    public void put(Object o) throws InterruptedException {
        Preconditions.checkNotNull(o);
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
            waitChildren();
        }
        return o;
    }

    @Override
    public Object poll(long timeout, TimeUnit unit) throws InterruptedException {
        checkState();
        long total = unit.toMillis(timeout);
        long start = System.currentTimeMillis();
        long waitMillis = total - (System.currentTimeMillis() - start);
        Object o = null;
        while ((o = poll()) == null && waitMillis > 0) {
            waitChildren(waitMillis, TimeUnit.MILLISECONDS);
            waitMillis = total - (System.currentTimeMillis() - start);
        }
        return o;
    }

    @Override
    public int drainTo(Collection<? super Object> c) {
        checkDrainToArgs(c);
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
        checkDrainToArgs(c);
        checkState();
        try {
            List<Object> all = ZkUtils.takeAllChildrenData(zk, queuePath, serializer, maxElements);
            c.addAll(all);
            return all.size();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void checkDrainToArgs(Collection<? super Object> c) {
        Preconditions.checkNotNull(c);
        Preconditions.checkArgument(c != this, "the specified collection is this queue");
    }

    /**
     * 阻塞，等待{@link #queuePath} 上的 {@link org.apache.zookeeper.Watcher.Event.EventType.NodeChildrenChanged} 事件发生
     * @param timeout
     * @param unit
     * @throws InterruptedException
     */
    protected void waitChildren(long timeout, TimeUnit unit) throws InterruptedException {
        CountDownLatch cdl = new CountDownLatch(1);
        EventLatchWatcher elw = new EventLatchWatcher(Watcher.Event.EventType.NodeChildrenChanged, cdl);
        try {
            zk.getChildren(queuePath, elw);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
        if (timeout == -1 && unit == null)
            cdl.await();
        else
            cdl.await(timeout, unit);
    }

    protected void waitChildren() throws InterruptedException {
        waitChildren(-1, null);
    }

    protected static class EventLatchWatcher implements Watcher {

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
