package com.nameof.zookeeper.util.lock;

import com.google.common.base.Preconditions;
import com.nameof.zookeeper.util.common.ZkContext;
import com.nameof.zookeeper.util.utils.ZkUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import static org.apache.zookeeper.Watcher.Event.KeeperState.Expired;
import static org.apache.zookeeper.Watcher.Event.KeeperState.SyncConnected;

/**
 * @Author: chengpan
 * @Date: 2018/11/11
 */
public abstract class BaseZkLock extends ZkContext implements Lock, Watcher {

    protected static final String NAMESPACE = "/zklock";

    protected  String lockPath;

    public BaseZkLock(String lockName, String connectString) throws IOException, InterruptedException, KeeperException {
        super(connectString);

        Preconditions.checkNotNull(lockName, "lockName null");
        Preconditions.checkArgument(!lockName.contains("/"), "lockName invalid");

        this.lockPath = NAMESPACE + "/" + lockName;

        checkState();
        ZkUtils.createPersist(zk, NAMESPACE);
        ZkUtils.createPersist(zk, lockPath);
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }
}
