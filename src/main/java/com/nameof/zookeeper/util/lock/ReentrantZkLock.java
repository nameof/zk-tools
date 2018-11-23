package com.nameof.zookeeper.util.lock;

import com.nameof.zookeeper.util.common.WaitDuration;
import com.nameof.zookeeper.util.common.ZkPrimitiveSupport;
import com.nameof.zookeeper.util.utils.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 线程安全，可重入的分布式排它锁，基于zookeeper意味着它是公平的锁<br><br>
 * @Author: chengpan
 * @Date: 2018/11/11
 */
public class ReentrantZkLock extends AbstractZkLock {

    private String nodeName;
    private ZkPrimitiveSupport zkPrimitiveSupport;

    public ReentrantZkLock(String lockName, String connectString) throws IOException, InterruptedException, KeeperException {
        super(lockName, connectString);
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
        checkState();
        try {
            lockInterruptiblyInternal();
        } catch (InterruptedException e) {
            unlock();
            throw e;
        } catch (Exception e) {
            unlock();
            throw new RuntimeException(e);
        }
    }

    private void lockInterruptiblyInternal() throws KeeperException, InterruptedException {
        prepareLock();
        Phaser phaser = new Phaser(1);
        do {
            String precedNodeName = ZkUtils.getSortedPrecedNodeName(zk, lockPath, nodeName);
            if (nodeName.equals(precedNodeName)) return;

            zkPrimitiveSupport.waitNotExists(phaser, lockPath + "/" + precedNodeName);
        } while (true);
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
        } catch (Exception e) {
            unlock();
            return false;
        }
    }

    private boolean tryLockInternal() throws KeeperException, InterruptedException {
        prepareLock();

        String precedNodeName = ZkUtils.getSortedPrecedNodeName(zk, lockPath, nodeName);
        if (nodeName.equals(precedNodeName)) return true;

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

    private boolean tryLockInternal(long time, TimeUnit unit) throws InterruptedException, KeeperException, TimeoutException {
        prepareLock();
        Phaser phaser = new Phaser(1);
        WaitDuration duration = WaitDuration.from(unit.toMillis(time));
        do {
            String precedNodeName = ZkUtils.getSortedPrecedNodeName(zk, lockPath, nodeName);
            if (nodeName.equals(precedNodeName)) return true;

            zkPrimitiveSupport.waitNotExists(phaser, lockPath + "/" + precedNodeName, duration);
        } while (true);
    }

    private void prepareLock() throws KeeperException, InterruptedException {
        if (nodeName == null) {
            String nodePath = zk.create(lockPath + "/", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            nodeName = nodePath.substring(nodePath.lastIndexOf("/") + 1);
        }
    }

    @Override
    public synchronized void unlock() {
        if (nodeName == null) return;
        checkState();

        try {
            ZkUtils.deleteNodeIgnoreInterrupt(zk, getNodePath());
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
        this.nodeName = null;
    }

    private String getNodePath() {
        return lockPath + "/" + nodeName;
    }
}
