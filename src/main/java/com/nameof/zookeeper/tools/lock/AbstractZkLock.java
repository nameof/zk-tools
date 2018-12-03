package com.nameof.zookeeper.tools.lock;

import com.google.common.base.Preconditions;
import com.nameof.zookeeper.tools.common.WaitDuration;
import com.nameof.zookeeper.tools.common.ZkContext;
import com.nameof.zookeeper.tools.common.ZkPrimitiveSupport;
import com.nameof.zookeeper.tools.utils.ZkUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * @Author: chengpan
 * @Date: 2018/11/11
 */
public abstract class AbstractZkLock extends ZkContext implements Lock, Watcher {

    protected static final String NAMESPACE = "/zklock";

    protected  String lockPath;

    /** 锁节点抢占到的序列号 */
    protected String nodeNameSequence;

    protected ZkPrimitiveSupport zkPrimitiveSupport;

    public AbstractZkLock(String lockName, String connectString) throws IOException, InterruptedException, KeeperException {
        super(connectString);
        checkArgs(lockName);
        init(lockName);
    }

    private void checkArgs(String lockName) {
        Preconditions.checkNotNull(lockName, "lockName null");
        Preconditions.checkArgument(!lockName.contains("/"), "lockName invalid");
    }

    private void init(String lockName) throws KeeperException, InterruptedException {
        this.lockPath = NAMESPACE + "/" + lockName;
        zkPrimitiveSupport = new ZkPrimitiveSupport(zk);

        checkState();
        ZkUtils.createPersist(zk, NAMESPACE);
        ZkUtils.createPersist(zk, lockPath);
    }

    /** 节点除序列号以外的完整路径 */
    protected abstract String getNodePathPrefix();

    /**
     * 根据自身的锁算法获取需要等待的zookeeper node name，获取锁成功时无需等待其它节点，则返回当前当前节点的node name
     * @return 需要等待的zookeeper node name，否则返回当前节点的node name
     */
    protected abstract String findLockWaitTarget() throws KeeperException, InterruptedException;

    protected abstract String getNodeName();

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
        checkState();
        try {
            lockInterruptiblyInternal();
        } catch (InterruptedException e) {
            unlock();
            throw e;
        } catch (KeeperException e) {
            unlock();
            throw new RuntimeException(e);
        }
    }

    protected void lockInterruptiblyInternal() throws KeeperException, InterruptedException {
        prepareLock();
        Phaser p = new Phaser(1);
        do {
            String waitTarget = findLockWaitTarget();
            if (getNodeName().equals(waitTarget))
                return;

            zkPrimitiveSupport.waitNotExists(p, lockPath + "/" + waitTarget);
        } while (true);
    }

    protected void prepareLock() throws KeeperException, InterruptedException {
        if (nodeNameSequence == null) {
            String prefix = getNodePathPrefix();
            nodeNameSequence = ZkUtils.createTempAndGetSeq(zk, prefix);
        }
    }

    @Override
    public synchronized boolean tryLock() {
        checkState();
        try {
            while (true) {
                try {
                    return tryLockInternal();
                } catch (InterruptedException ignore) { }
            }
        } catch (KeeperException e) {
            unlock();
            return false;
        }
    }

    protected boolean tryLockInternal() throws KeeperException, InterruptedException {
        prepareLock();

        String waitTarget = findLockWaitTarget();
        if (getNodeName().equals(waitTarget))
            return true;

        unlock();
        return false;
    }

    @Override
    public synchronized boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        checkState();
        try {
            return tryLockInternal(time, unit);
        } catch (TimeoutException e) {
            unlock();
            return false;
        }  catch (InterruptedException e) {
            unlock();
            throw e;
        } catch (Exception e) {
            unlock();
            throw new RuntimeException(e);
        }
    }

    protected boolean tryLockInternal(long time, TimeUnit unit) throws KeeperException, InterruptedException, TimeoutException {
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
    }

    @Override
    public synchronized void unlock() {
        if (nodeNameSequence == null) return;
        checkState();

        try {
            ZkUtils.deleteNodeIgnoreInterrupt(zk, getNodePath());
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
        this.nodeNameSequence = null;
    }

    protected String getNodePath() {
        return lockPath + "/" + getNodeName();
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }
}
