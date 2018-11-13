package com.nameof.zookeeper.util.queue;

import com.google.common.base.Preconditions;
import com.nameof.zookeeper.util.utils.ZkUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

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
        Phaser phaser = new Phaser(1);
        while ((o = poll()) == null) {
            waitChildren(phaser);
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
        Phaser phaser = new Phaser(1);
        while ((o = poll()) == null && waitMillis > 0) {
            try {
                waitChildren(phaser, waitMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                break;
            }
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
     * @param phaser
     * @param timeout
     * @param unit
     * @throws InterruptedException
     */
    protected void waitChildren(Phaser phaser, long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        EventPhaserWatcher epw = new EventPhaserWatcher(Watcher.Event.EventType.NodeChildrenChanged, phaser);
        try {
            zk.getChildren(queuePath, epw);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
        if (timeout == -1 && unit == null)
            phaser.awaitAdvance(phaser.getPhase());
        else
            phaser.awaitAdvanceInterruptibly(phaser.getPhase(), timeout, unit);
    }

    protected void waitChildren(Phaser phaser) throws InterruptedException {
        try {
            waitChildren(phaser, -1, null);
        } catch (TimeoutException ignore) {
            //never happen
        }
    }
}
