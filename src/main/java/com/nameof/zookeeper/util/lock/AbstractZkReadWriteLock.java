package com.nameof.zookeeper.util.lock;

import com.nameof.zookeeper.util.common.WaitDuration;
import com.nameof.zookeeper.util.common.ZkPrimitiveSupport;
import com.nameof.zookeeper.util.utils.ZkUtils;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @Author: chengpan
 * @Date: 2018/11/26
 */
public abstract class AbstractZkReadWriteLock extends AbstractZkLock {
    protected static final String READ_PREFIX = "READ-";
    protected static final String WRITE_PREFIX = "WRITE-";

    protected ReentrantZkReadWriteLock context;
    protected ReadWriteLockState lockState;

    protected String nodeNameSequence;

    protected ZkPrimitiveSupport zkPrimitiveSupport;

    public AbstractZkReadWriteLock(String lockName, ReadWriteLockState lockState, ReentrantZkReadWriteLock context, String connectString) throws IOException, InterruptedException, KeeperException {
        super(lockName, connectString);
        this.lockState = lockState;
        this.context = context;
        zkPrimitiveSupport = new ZkPrimitiveSupport(zk);
    }

    @Override
    public synchronized void lock() {
        while (true) {
            try {
                lockInterruptibly();
                return;
            } catch (InterruptedException ignore) { }
        }
    }

    @Override
    public synchronized void lockInterruptibly() throws InterruptedException {
        try {
            prepareLock();
            Phaser p = new Phaser(1);
            do {
                String waitTarget = findLockWaitTarget();
                if (getNodeName().equals(waitTarget))
                    return;

                zkPrimitiveSupport.waitNotExists(p, lockPath + "/" + waitTarget);
            } while (true);
        } catch (KeeperException e) {
            unlock();
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据自身的锁算法获取需要等待的zookeeper node name，获取锁成功时无需等待其它节点，则返回当前当前节点的node name
     * @return 需要等待的zookeeper node name，否则返回当前节点的node name
     */
    protected abstract String findLockWaitTarget() throws KeeperException, InterruptedException;

    private void prepareLock() throws KeeperException, InterruptedException {
        lockState.acceptLockState(context);
        if (nodeNameSequence == null) {
            String prefix = getNodePathPrefix();
            nodeNameSequence = ZkUtils.createTempAndGetSeq(zk, prefix);
        }
    }

    protected abstract String getNodePathPrefix();

    @Override
    public synchronized boolean tryLock() {
        try {
            while (true) {
                try {
                    return tryLockInternal();
                } catch (InterruptedException ignore) { }
            }
        } catch (KeeperException e) {
            unlock();
            throw new RuntimeException(e);
        }
    }

    private boolean tryLockInternal() throws KeeperException, InterruptedException {
        prepareLock();
        String waitTarget = findLockWaitTarget();

        if (getNodeName().equals(waitTarget))
            return true;

        unlock();
        return false;
    }

    @Override
    public synchronized boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        try {
            prepareLock();
            Phaser phaser = new Phaser(1);
            WaitDuration duration = WaitDuration.from(unit.toMillis(time));
            do {
                String waitTarget = findLockWaitTarget();
                if (getNodeName().equals(waitTarget))
                    return true;

                zkPrimitiveSupport.waitNotExists(phaser, lockPath + "/" + waitTarget
                        , duration);
            } while (true);
        } catch (KeeperException e) {
            unlock();
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            unlock();
            return false;
        }
    }

    @Override
    public synchronized void unlock() {
        ReadWriteLockState.NONE.acceptLockState(context);

        if (nodeNameSequence == null) return;
        checkState();

        try {
            zk.delete(getNodePath(), -1);
        } catch (KeeperException.NoNodeException e) {
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.nodeNameSequence = null;
    }

    protected String getNodePath() {
        return  lockPath + "/" + getNodeName();
    }

    protected abstract String getNodeName();
}
