package com.nameof.zookeeper.util.queue;

import com.google.common.base.Preconditions;
import com.nameof.zookeeper.util.utils.ZkUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;

public class ZkQueue extends BaseQueue {

    private static final String NAMESPACE = "/zkqueue";

    private Serializer serializer;

    private String queuePath;

    private ZooKeeper zk;

    private volatile Watcher.Event.KeeperState zkState;

    public ZkQueue(String queueName, String connectString, Serializer serializer) throws IOException, InterruptedException, KeeperException {
        Preconditions.checkNotNull(queueName, "queueName null");
        Preconditions.checkArgument(queueName.contains("/"), "queueName invalid");
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

    public int size() {
        checkState();
        try {
            return zk.getChildren(queuePath, null).size();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isEmpty() {
        checkState();
        try {
            return this.size() == 0;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

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
        checkState();
        for (Object o:
             c) {
            this.add(o);
        }
        return true;
    }

    public void clear() {
        checkState();
        try {
            ZkUtils.deleteChildren(zk, queuePath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean offer(Object o) {
        checkState();
        this.add(o);
        return true;
    }

    public Object remove() {
        checkState();
        try {
            for(;;) {
                String min = ZkUtils.getMinSeqChildren(zk, "", queuePath);
                if (min == null) throw new NoSuchElementException();
                try {
                    byte[] data = zk.getData(queuePath + "/" + min, false, null);
                    Object o = serializer.deserialize(data);
                    ZkUtils.deleteChildren(zk, queuePath + "/" + min);
                    return o;
                } catch (KeeperException.NoNodeException ignore) { }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Object poll() {
        checkState();
        try {
            for(;;) {
                String min = ZkUtils.getMinSeqChildren(zk, "", queuePath);
                if (min == null) return null;
                try {
                    byte[] data = zk.getData(queuePath + "/" + min, false, null);
                    Object o = serializer.deserialize(data);
                    ZkUtils.deleteChildren(zk, queuePath + "/" + min);
                    return o;
                } catch (KeeperException.NoNodeException ignore) { }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Object element() {
        checkState();
        try {
            String min = ZkUtils.getMinSeqChildren(zk, "", queuePath);
            if (min == null) throw new NoSuchElementException();
            byte[] data = zk.getData(queuePath + "/" + min, false, null);
            return serializer.deserialize(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Object peek() {
        checkState();
        try {
            String min = ZkUtils.getMinSeqChildren(zk, "", queuePath);
            if (min == null) return null;
            byte[] data = zk.getData(queuePath + "/" + min, false, null);
            return serializer.deserialize(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
