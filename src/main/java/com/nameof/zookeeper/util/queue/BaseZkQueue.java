package com.nameof.zookeeper.util.queue;

import com.google.common.base.Preconditions;
import com.nameof.zookeeper.util.common.ZkContext;
import com.nameof.zookeeper.util.utils.ZkUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.util.*;

/**
 * 基于zookeeper实现的基本无界队列
 * @author chengpan
 */
public abstract class BaseZkQueue extends ZkContext implements Queue<Object>, Watcher {

    protected static final String NAMESPACE = "/zkqueue";

    protected Serializer serializer;

    protected String queuePath;

    public BaseZkQueue(String queueName, String connectString, Serializer serializer) throws IOException, InterruptedException, KeeperException {
        super(connectString);
        checkArgs(queueName, serializer);
        this.queuePath = NAMESPACE + "/" + queueName;
        this.serializer = serializer;
        init();
    }

    private void checkArgs(String queueName, Serializer serializer) {
        Preconditions.checkNotNull(queueName, "queueName null");
        Preconditions.checkArgument(!queueName.contains("/"), "queueName invalid");
        Preconditions.checkNotNull(serializer, "serializer null");
    }

    private void init() throws KeeperException, InterruptedException {
        checkState();
        ZkUtils.createPersist(zk, NAMESPACE);
        ZkUtils.createPersist(zk, queuePath);
    }

    @Override
    public boolean add(Object o) {
        return offer(o);
    }

    @Override
    public boolean addAll(Collection<?> c) {
        Preconditions.checkNotNull(c);
        for (Object o: c) {
            this.add(o);
        }
        return true;
    }

    @Override
    public boolean offer(Object o) {
        Preconditions.checkNotNull(o);
        checkState();
        try {
            ZkUtils.crecatePersistSeq(zk, queuePath + "/", serializer.serialize(o));
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
    public void clear() {
        checkState();
        try {
            ZkUtils.deleteChildren(zk, queuePath);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isEmpty() {
        return this.size() == 0;
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
        Preconditions.checkNotNull(a);
        checkState();
        Object[] objs = this.toArray();
        if (a.length < objs.length)
            return Arrays.copyOf(objs, objs.length, (Class<? extends T[]>) a.getClass());
        System.arraycopy(objs, 0, a, 0, objs.length);
        if (a.length > objs.length)
            a[objs.length] = null;
        return a;
    }

    @Override
    public Object remove() {
        checkState();
        try {
            for(;;) {
                String min = ZkUtils.getMinSeqChild(zk, queuePath);
                if (min == null) throw new NoSuchElementException();
                try {
                    return ZkUtils.getNodeDataWithDelete(zk, queuePath + "/" + min, serializer);
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
                String min = ZkUtils.getMinSeqChild(zk, queuePath);
                if (min == null) return null;
                try {
                    return ZkUtils.getNodeDataWithDelete(zk, queuePath + "/" + min, serializer);
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
            String min = ZkUtils.getMinSeqChild(zk, queuePath);
            if (min == null) throw new NoSuchElementException();
            return ZkUtils.getNodeData(zk, queuePath + "/" + min, serializer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object peek() {
        checkState();
        try {
            String min = ZkUtils.getMinSeqChild(zk, queuePath);
            if (min == null) return null;
            return ZkUtils.getNodeData(zk, queuePath + "/" + min, serializer);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }
}
