package com.nameof.zookeeper.util.lock;

import com.nameof.zookeeper.util.common.WaitDuration;
import com.nameof.zookeeper.util.common.ZkPrimitiveSupport;
import com.nameof.zookeeper.util.utils.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * 可重入的分布式读写锁
 * 不支持锁升级、降级和撤销
 * @Author: chengpan
 * @Date: 2018/11/19
 */
public class ReentrantZkReadWriteLock implements ReadWriteLock {

    private static final String READ_PREFIX = "read-";
    private static final String WRITE_PREFIX = "write-";

    private final Lock readLock;
    private final Lock writeLock;

    public ReentrantZkReadWriteLock(String lockName, String connectString) throws InterruptedException, IOException, KeeperException {
        this.readLock = new ReadLock(lockName, connectString);
        this.writeLock = new WriteLock(lockName, connectString);
    }

    public Lock readLock() { return readLock; }
    public Lock writeLock() { return writeLock; }


    private static class ReadLock extends AbstractZkLock {

        private String nodeName;
        private ZkPrimitiveSupport zkPrimitiveSupport;

        public ReadLock(String lockName, String connectString) throws IOException, InterruptedException, KeeperException {
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
            try {
                prepareLock();
                Phaser p = new Phaser(1);
                do {
                    String precedWriter = getPrecedWriter();
                    if (nodeName.equals(precedWriter))
                        return;

                    zkPrimitiveSupport.waitNotExists(p, lockPath + "/" + precedWriter);
                } while (true);
            } catch (KeeperException e) {
                unlock();
                throw new RuntimeException(e);
            }
        }

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
            String precedWriter = getPrecedWriter();

            if (nodeName.equals(precedWriter))
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
                    String precedWriter = getPrecedWriter();
                    if (nodeName.equals(precedWriter))
                        return true;

                    zkPrimitiveSupport.waitNotExists(phaser, lockPath + "/" + READ_PREFIX + precedWriter
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

        private void prepareLock() throws KeeperException, InterruptedException {
            if (nodeName == null) {
                String prefix = lockPath + "/" + READ_PREFIX;
                String nodePath = zk.create(prefix, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                nodeName = nodePath.substring(prefix.length());
            }
        }

        private String getPrecedWriter() throws KeeperException, InterruptedException {
            List<String> children = ZkUtils.getChildren(zk, lockPath);
            for (String child : children) {
                if (child.startsWith(WRITE_PREFIX)) {
                    child = child.substring(WRITE_PREFIX.length());
                    if (child.compareTo(nodeName) < 0)
                        return WRITE_PREFIX + child;
                }
            }
            return nodeName;
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
            return lockPath + "/" + getRealNodeName();
        }

        private String getRealNodeName() { return READ_PREFIX + nodeName; }
    }

    private static class WriteLock extends AbstractZkLock {

        private String nodeName;
        private ZkPrimitiveSupport zkPrimitiveSupport;

        public WriteLock(String lockName, String connectString) throws IOException, InterruptedException, KeeperException {
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
            try {
                prepareLock();
                Phaser p = new Phaser(1);
                do {
                    String precedNodeName = getPreceNodeName();
                    if (nodeName.equals(precedNodeName))
                        return;

                    zkPrimitiveSupport.waitNotExists(p, lockPath + "/" + precedNodeName);
                } while (true);
            } catch (KeeperException e) {
                unlock();
                throw new RuntimeException(e);
            }
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
                if (child.compareTo(nodeName) < 0) {
                    return read ? READ_PREFIX + child : WRITE_PREFIX + child;
                }
            }
            return nodeName;
        }

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
            String preceNodeName = getPreceNodeName();

            if (nodeName.equals(preceNodeName))
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
                    String preceNodeName = getPreceNodeName();
                    if (nodeName.equals(preceNodeName))
                        return true;

                    zkPrimitiveSupport.waitNotExists(phaser, lockPath + "/" + preceNodeName
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

        private void prepareLock() throws KeeperException, InterruptedException {
            if (nodeName == null) {
                String prefix = lockPath + "/" + WRITE_PREFIX;
                String nodePath = zk.create(prefix, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                nodeName = nodePath.substring(prefix.length());
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
            return lockPath + "/" + getRealNodeName();
        }

        private String getRealNodeName() { return WRITE_PREFIX + nodeName; }
    }
}
