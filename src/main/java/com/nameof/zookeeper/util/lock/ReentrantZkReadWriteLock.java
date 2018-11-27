package com.nameof.zookeeper.util.lock;

import com.nameof.zookeeper.util.common.ZkContext;
import com.nameof.zookeeper.util.utils.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * 可重入的分布式读写锁
 * 不支持锁升级、降级和撤销
 * @Author: chengpan
 * @Date: 2018/11/19
 */
public class ReentrantZkReadWriteLock implements ReadWriteLock {

    private Lock readLock;
    private Lock writeLock;

    private ReadWriteLockState lockState = ReadWriteLockState.NONE;

    public ReentrantZkReadWriteLock(String lockName, String connectString) throws InterruptedException, IOException, KeeperException {
        this.readLock = new ReadLock(lockName, connectString);
        this.writeLock = new WriteLock(lockName, connectString);
    }

    public Lock readLock() { return readLock; }
    public Lock writeLock() { return writeLock; }

    void setLockState(ReadWriteLockState lockState) { this.lockState = lockState; }
    public ReadWriteLockState getLockState() { return lockState; }

    public void destory() {
        ((ZkContext) readLock).destory();
        ((ZkContext) writeLock).destory();
    }


    private class ReadLock extends AbstractZkReadWriteLock {

        public ReadLock(String lockName, String connectString) throws IOException, InterruptedException, KeeperException {
            super(lockName, ReadWriteLockState.READ, ReentrantZkReadWriteLock.this, connectString);
        }

        @Override
        protected String findLockWaitTarget() throws KeeperException, InterruptedException {
            return getPrecedWriter();
        }

        private String getPrecedWriter() throws KeeperException, InterruptedException {
            List<String> children = ZkUtils.getChildren(zk, lockPath);
            for (String child : children) {
                if (child.startsWith(WRITE_PREFIX)) {
                    child = child.substring(WRITE_PREFIX.length());
                    if (child.compareTo(nodeNameSequence) < 0)
                        return WRITE_PREFIX + child;
                }
            }
            return getNodeName();
        }

        @Override
        protected String getNodePathPrefix() {
            return lockPath + "/" + READ_PREFIX;
        }

        @Override
        protected String getNodeName() {
            return READ_PREFIX + nodeNameSequence;
        }
    }

    private class WriteLock extends AbstractZkReadWriteLock {

        public WriteLock(String lockName, String connectString) throws IOException, InterruptedException, KeeperException {
            super(lockName, ReadWriteLockState.WRITE, ReentrantZkReadWriteLock.this, connectString);
        }

        @Override
        protected String findLockWaitTarget() throws KeeperException, InterruptedException {
            return getPreceNodeName();
        }

        private String getPreceNodeName() throws KeeperException, InterruptedException {
            List<String> children = ZkUtils.getChildren(zk, lockPath);
            for (String child : children) {
                boolean read = true;
                if (child.startsWith(WRITE_PREFIX)) {
                    read = false;
                    child = child.substring(WRITE_PREFIX.length());
                } else {
                    child = child.substring(READ_PREFIX.length());
                }
                if (child.compareTo(nodeNameSequence) < 0) {
                    return read ? READ_PREFIX + child : WRITE_PREFIX + child;
                }
            }
            return getNodeName();
        }

        @Override
        protected String getNodePathPrefix() {
            return lockPath + "/" + WRITE_PREFIX;
        }

        @Override
        protected String getNodeName() {
            return WRITE_PREFIX + nodeNameSequence;
        }
    }
}
