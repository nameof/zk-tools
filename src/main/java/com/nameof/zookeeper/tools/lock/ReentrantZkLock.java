package com.nameof.zookeeper.tools.lock;

import com.nameof.zookeeper.tools.common.ZkPrimitiveSupport;
import com.nameof.zookeeper.tools.utils.ZkUtils;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 * 线程安全，可重入的分布式排它锁，基于zookeeper意味着它是公平的锁<br><br>
 * @Author: chengpan
 * @Date: 2018/11/11
 */
public class ReentrantZkLock extends AbstractZkLock {

    public ReentrantZkLock(String lockName, String connectString) throws IOException, InterruptedException, KeeperException {
        super(lockName, connectString);
    }

    @Override
    protected String findLockWaitTarget() throws KeeperException, InterruptedException {
        return ZkUtils.getSortedPrecedNodeName(zk, lockPath, nodeNameSequence);
    }

    @Override
    protected String getNodeName() {
        return nodeNameSequence;
    }

    @Override
    protected String getNodePathPrefix() {
        return lockPath + "/";
    }
}
