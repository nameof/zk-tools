package com.nameof.zookeeper.tools.lock;

import com.nameof.zookeeper.tools.common.ZkPrimitiveSupport;
import com.nameof.zookeeper.tools.utils.ZkUtils;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @Author: chengpan
 * @Date: 2018/11/26
 */
public abstract class AbstractZkReadWriteLock extends AbstractZkLock {
    protected static final String READ_PREFIX = "READ-";
    protected static final String WRITE_PREFIX = "WRITE-";

    protected ReentrantZkReadWriteLock context;
    protected ReadWriteLockState lockState;

    public AbstractZkReadWriteLock(String lockName, ReadWriteLockState lockState, ReentrantZkReadWriteLock context, String connectString) throws IOException, InterruptedException, KeeperException {
        super(lockName, connectString);
        zkPrimitiveSupport = new ZkPrimitiveSupport(zk);
        this.lockState = lockState;
        this.context = context;
    }

    @Override
    protected void prepareLock() throws KeeperException, InterruptedException {
        lockState.acceptLockState(context);
        super.prepareLock();
    }

    @Override
    public synchronized void unlock() {
        ReadWriteLockState.NONE.acceptLockState(context);
        super.unlock();
    }

    protected List<ReadWriteLockNodeEntry> getSortedNodeEntry() throws KeeperException, InterruptedException {
        List<String> children = ZkUtils.getChildren(zk, lockPath);
        List<ReadWriteLockNodeEntry> entryList = children.stream().map(s -> {
            return ReadWriteLockNodeEntry.fromNodeName(s);
        }).collect(Collectors.toList());

        Collections.sort(entryList);
        return entryList;
    }

    protected static class ReadWriteLockNodeEntry implements Comparable<ReadWriteLockNodeEntry> {
        private boolean readNode;
        private String nodeSeq;

        public ReadWriteLockNodeEntry(boolean readNode, String nodeSeq) {
            this.readNode = readNode;
            this.nodeSeq = nodeSeq;
        }

        public static ReadWriteLockNodeEntry fromNodeName(String nodeName) {
            if (nodeName.startsWith(WRITE_PREFIX)) {
                return new ReadWriteLockNodeEntry(false, nodeName.substring(WRITE_PREFIX.length()));
            }
            return new ReadWriteLockNodeEntry(true, nodeName.substring(READ_PREFIX.length()));
        }

        public String getNodeName() {
            return readNode ? READ_PREFIX + nodeSeq : WRITE_PREFIX + nodeSeq;
        }

        public String getNodeSeq() {
            return nodeSeq;
        }

        public boolean isReadNode() {
            return readNode;
        }

        @Override
        public int compareTo(ReadWriteLockNodeEntry o) {
            return nodeSeq.compareTo(o.getNodeSeq());
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ReadWriteLockNodeEntry))
                return false;
            ReadWriteLockNodeEntry entry = (ReadWriteLockNodeEntry) obj;
            return Objects.equals(entry.getNodeSeq(), getNodeSeq());
        }
    }
}
