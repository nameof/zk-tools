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
public class ZkQueue extends BaseQueue {

    private static final String NAMESPACE = "/zkqueue";

    private Serializer serializer;

    private String queuePath;

    private ZooKeeper zk;

    private volatile Watcher.Event.KeeperState zkState;

    public ZkQueue(String queueName, String connectString, Serializer serializer) throws IOException, InterruptedException, KeeperException {
        Preconditions.checkNotNull(queueName, "queueName null");
        Preconditions.checkArgument(!queueName.contains("/"), "queueName invalid");
        Preconditions.checkNotNull(connectString, "connectString null");
        Preconditions.checkNotNull(serializer, "serializer null");

        this.queuePath = NAMESPACE + "/" + queueName;
        this.serializer = serializer;

        CountDownLatch cdl = new CountDownLatch(1);
        zk = new ZooKeeper(connectString, 10_000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getType() == Event.EventType.None) {
                    ZkQueue.this.zkState = event.getState();
                    cdl.countDown();
                }
            }
        });
        try {
            cdl.await();
        } catch (InterruptedException e) {
            zk.close();
            throw e;
        }

        checkState();
        ZkUtils.createPersist(zk, NAMESPACE);
        ZkUtils.createPersist(zk, queuePath);
    }

    @Override
    public int size() {
        checkState();
        try {
            return zk.getChildren(queuePath, null).size();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isEmpty() {
        return this.size() == 0;
    }

    @Override
    public boolean add(Object o) {
        checkState();
        try {
            ZkUtils.crecatePersistSeq(zk, queuePath + "/", serializer.serialize(o));
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean addAll(Collection<?> c) {
        for (Object o: c) {
            this.add(o);
        }
        return true;
    }

    @Override
    public void clear() {
        checkState();
        try {
            ZkUtils.deleteChildren(zk, queuePath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean offer(Object o) {
        this.add(o);
        return true;
    }

    @Override
    public Object remove() {
        checkState();
        try {
            for(;;) {
                String min = ZkUtils.getMinSeqChildren(zk, queuePath);
                if (min == null) throw new NoSuchElementException();
                try {
                    byte[] data = zk.getData(queuePath + "/" + min, false, null);
                    Object o = serializer.deserialize(data);
                    ZkUtils.delete(zk, queuePath + "/" + min);
                    return o;
                } catch (KeeperException.NoNodeException ignore) { }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object poll() {
        checkState();
        try {
            for(;;) {
                String min = ZkUtils.getMinSeqChildren(zk, queuePath);
                if (min == null) return null;
                try {
                    byte[] data = zk.getData(queuePath + "/" + min, false, null);
                    Object o = serializer.deserialize(data);
                    ZkUtils.delete(zk, queuePath + "/" + min);
                    return o;
                } catch (KeeperException.NoNodeException ignore) { }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object element() {
        checkState();
        try {
            String min = ZkUtils.getMinSeqChildren(zk, queuePath);
            if (min == null) throw new NoSuchElementException();
            byte[] data = zk.getData(queuePath + "/" + min, false, null);
            return serializer.deserialize(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object peek() {
        checkState();
        try {
            String min = ZkUtils.getMinSeqChildren(zk, queuePath);
            if (min == null) return null;
            byte[] data = zk.getData(queuePath + "/" + min, false, null);
            return serializer.deserialize(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object[] toArray() {
        checkState();
        try {
            return ZkUtils.getAllChildrenData(zk, queuePath, serializer).toArray();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T[] toArray(T[] a) {
        Object[] objs = this.toArray();
        if (a.length < objs.length)
            return Arrays.copyOf(objs, objs.length, (Class<? extends T[]>) a.getClass());
        System.arraycopy(objs, 0, a, 0, objs.length);
        if (a.length > objs.length)
            a[objs.length] = null;
        return a;
    }

    private void checkState() {
        switch(zkState) {
            case SyncConnected:
                return;
            case Expired:
                //create new client ?
            default:
                throw new IllegalStateException("zookeeper state : " + zkState);
        }
    }
}
