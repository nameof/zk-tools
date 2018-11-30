package com.nameof.zookeeper.tools.queue;

import com.google.common.base.Preconditions;
import com.nameof.zookeeper.tools.common.ZkPrimitiveSupport;
import com.nameof.zookeeper.tools.common.WaitDuration;
import com.nameof.zookeeper.tools.utils.ZkUtils;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 无界阻塞队列
 * @author chengpan
 */
public class ZkBlockingQueue extends AbstractZkBlockingQueue {

    protected ZkPrimitiveSupport zkPrimitiveSupport;

    public ZkBlockingQueue(String queueName, String connectString, Serializer serializer) throws IOException, InterruptedException, KeeperException {
        super(queueName, connectString, serializer);
        zkPrimitiveSupport = new ZkPrimitiveSupport(zk);
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
        Phaser phaser = new Phaser(1);
        while ((o = poll()) == null) {
            zkPrimitiveSupport.waitChildren(phaser, queuePath);
        }
        return o;
    }

    @Override
    public Object poll(long timeout, TimeUnit unit) throws InterruptedException {
        checkState();
        Object o = null;
        WaitDuration duration = WaitDuration.from(unit.toMillis(timeout));
        Phaser phaser = new Phaser(1);
        while ((o = poll()) == null) {
            try {
                zkPrimitiveSupport.waitChildren(phaser, queuePath, duration);
            } catch (TimeoutException e) {
                break;
            }
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
}
