package com.nameof.zookeeper.util.lock;

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

    public ReentrantZkLock(String lockName, String connectString) throws IOException, InterruptedException, KeeperException {
        super(lockName, connectString);
    }

    @Override
    public synchronized void lock() {
        while (true) {
            try {
                lockInterruptibly();
                return;
            } catch (InterruptedException ignore) {
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
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
        Phaser phaser = new Phaser(1);
        do {
            prepareLock();

            String previousNodeName = ZkUtils.getSortedPreviousNodeName(zk, lockPath, nodeName);
            if (nodeName.equals(previousNodeName)) return;

            waitToGetLock(phaser, lockPath + "/" + previousNodeName);
        } while (true);
    }

    @Override
    public synchronized boolean tryLock() {
        checkState();
        try {
            return tryLockInternal();
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
        long maxWaitMills = unit.toMillis(time);
        long start = System.currentTimeMillis();
        Phaser phaser = new Phaser(1);
        do {
            prepareLock();

            String previousNodeName = ZkUtils.getSortedPreviousNodeName(zk, lockPath, nodeName);
            if (nodeName.equals(previousNodeName)) return true;

            long waitMillis = maxWaitMills - (System.currentTimeMillis() - start);
            if (waitMillis <= 0)
                break;

            waitToGetLock(phaser, lockPath + "/" + previousNodeName, waitMillis, TimeUnit.MILLISECONDS);
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

    private void waitToGetLock(Phaser phaser, String path) throws KeeperException, InterruptedException {
        try {
            waitToGetLock(phaser, path, -1, null);
        } catch (TimeoutException e) { }
    }

    private void waitToGetLock(Phaser phaser, String path, long time, TimeUnit unit) throws KeeperException, InterruptedException, TimeoutException {
        EventPhaserWatcher epw = new EventPhaserWatcher(Event.EventType.NodeDeleted, phaser);
        Stat exists = zk.exists(path, epw);
        if (exists == null) {
            return;
        }
        if (time == -1 && unit == null)
            phaser.awaitAdvance(phaser.getPhase());
        else
            phaser.awaitAdvanceInterruptibly(phaser.getPhase(), time, unit);
    }

    private String getNodePath() {
        return lockPath + "/" + nodeName;
    }
}
