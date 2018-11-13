package com.nameof.zookeeper.util.barrier;

import com.google.common.base.Preconditions;
import com.nameof.zookeeper.util.common.ZkContext;
import com.nameof.zookeeper.util.utils.ZkUtils;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>zookeeper官方的Barrier代码示例存在几个BUG，可能导致客户端永久阻塞：
 *     <p>1) 使用Object.wait , notify机制唤醒客户端，https://issues.apache.org/jira/browse/ZOOKEEPER-3186，这里使用CountDownLatch替代
 *     <p>2) enter和leave的事件通知竞态产生的ABA问题，https://issues.apache.org/jira/browse/ZOOKEEPER-1011，通过ready节点解决
 * <p><p>thread-safe
 * @Author: chengpan
 * @Date: 2018/11/8
 */
public class ZkBarrier extends ZkContext implements Barrier, Watcher {
    private static final String NAMESPACE = "/zkbarrier";

    private int size;

    private String barrierPath;

    private String barrierReadyPath;

    private String nodeName = UUID.randomUUID().toString();

    private AtomicBoolean allReady = new AtomicBoolean(false);

    public ZkBarrier(String barrierName, String connectString, int size) throws IOException, InterruptedException, KeeperException {
        super(connectString);
        checkArgs(barrierName, size);

        this.barrierPath = NAMESPACE + "/" + barrierName;
        this.barrierReadyPath = NAMESPACE + "/" + barrierName + "_ready";
        this.size = size;

        init();
    }

    private void checkArgs(String barrierName, int size) {
        Preconditions.checkNotNull(barrierName, "barrierName null");
        Preconditions.checkArgument(!barrierName.contains("/"), "barrierName invalid");
        Preconditions.checkNotNull(size > 0, "size invalid");
    }

    private void init() throws KeeperException, InterruptedException {
        checkState();
        ZkUtils.createPersist(zk, NAMESPACE);
        ZkUtils.createPersist(zk, barrierPath);
    }

    @Override
    public synchronized boolean enter() throws Exception {
        checkState();

        try {
            zk.create(barrierPath + "/" + nodeName, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException.NodeExistsException e) {
            return false;
        }

        if (ready()) return true;

        List<String> list = zk.getChildren(barrierPath, false);
        while (!allReady.get() && list.size() < size) {
            wait();
        }
        ZkUtils.createPersist(zk, barrierReadyPath);
        return true;
    }

    private boolean ready() throws KeeperException, InterruptedException {
        return zk.exists(barrierReadyPath, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                allReady.set(true);
                synchronized (ZkBarrier.this) {
                    notifyAll();
                }
            }
        }) != null;
    }

    @Override
    public synchronized boolean leave() throws Exception {
        checkState();

        try {
            zk.delete(barrierPath + "/" + nodeName, -1);
        } catch (KeeperException.NoNodeException e) {
            return false;
        }

        Phaser phaser = new Phaser(1);
        while (true) {
            EventPhaserWatcher epw = new EventPhaserWatcher(Watcher.Event.EventType.NodeChildrenChanged, phaser);
            List<String> list = zk.getChildren(barrierPath, epw);
            if (list.size() > 0) {
                phaser.awaitAdvance(phaser.getPhase());
            } else {
                cleanup();
                return true;
            }
        }
    }

    private void cleanup() throws KeeperException, InterruptedException {
        try {
            zk.delete(barrierReadyPath, -1);
        } catch (KeeperException.NoNodeException ignore) { } finally {
            allReady.set(false);
        }
    }
}
