package com.nameof.zookeeper.util.lock;

import com.nameof.zookeeper.util.common.ZkPrimitiveSupport;
import com.nameof.zookeeper.util.utils.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 线程安全，可重入的分布式排它锁<br><br>
 * @Author: chengpan
 * @Date: 2018/11/11
 */
public class ReentrantZkLock extends BaseZkLock {

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
            String previousNodeName = ZkUtils.getSortedPreviousNodeName(zk, lockPath, nodeName);
            if (nodeName.equals(previousNodeName)) return;

            zkPrimitiveSupport.waitNotExists(phaser, lockPath + "/" + previousNodeName);
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

        String previousNodeName = ZkUtils.getSortedPreviousNodeName(zk, lockPath, nodeName);
        if (nodeName.equals(previousNodeName)) return true;

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

    private boolean tryLockInternal(long time, TimeUnit unit) throws InterruptedException, TimeoutException, KeeperException {
        prepareLock();
        long maxWaitMills = unit.toMillis(time);
        long start = System.currentTimeMillis();
        Phaser phaser = new Phaser(1);
        do {
            String previousNodeName = ZkUtils.getSortedPreviousNodeName(zk, lockPath, nodeName);
            if (nodeName.equals(previousNodeName)) return true;

            long waitMillis = maxWaitMills - (System.currentTimeMillis() - start);
            if (waitMillis <= 0)
                break;

            zkPrimitiveSupport.waitNotExists(phaser, lockPath + "/" + previousNodeName, waitMillis, TimeUnit.MILLISECONDS);
        } while (true);

        unlock();
        return false;
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
            zk.delete(getNodePath(), -1);
        } catch (KeeperException.NoNodeException e) {
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        this.nodeName = null;
    }

    private String getNodePath() {
        return lockPath + "/" + nodeName;
    }
}
