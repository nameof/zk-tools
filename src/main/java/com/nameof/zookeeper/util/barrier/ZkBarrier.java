package com.nameof.zookeeper.util.barrier;

import com.google.common.base.Preconditions;
import com.nameof.zookeeper.util.utils.ZkUtils;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * TODO 处理重入
 * @Author: chengpan
 * @Date: 2018/11/8
 */
public class ZkBarrier implements Barrier, Watcher {
    private static final String NAMESPACE = "/zkbarrier";

    private int size;

    private String barrierPath;

    private String nodeName;

    private ZooKeeper zk;

    protected volatile Watcher.Event.KeeperState zkState;

    CountDownLatch cdl = new CountDownLatch(1);

    Object mutex = new Object();

    public ZkBarrier(String barrierName, String connectString, int size) throws IOException, InterruptedException, KeeperException {
        Preconditions.checkNotNull(barrierName, "barrierName null");
        Preconditions.checkArgument(!barrierName.contains("/"), "barrierName invalid");
        Preconditions.checkNotNull(connectString, "connectString null");
        Preconditions.checkNotNull(size > 0, "size invalid");

        this.barrierPath = NAMESPACE + "/" + barrierName;
        this.size = size;

        zk = new ZooKeeper(connectString, 10_000, this);
        try {
            cdl.await();
        } catch (InterruptedException e) {
            zk.close();
            throw e;
        }

        checkState();
        ZkUtils.createPersist(zk, NAMESPACE);
        ZkUtils.createPersist(zk, barrierPath);
    }

    @Override
    public void enter() throws Exception {
        this.nodeName = zk.create(barrierPath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        Object mutex = new Object();
        while (true) {
            synchronized (mutex) {
                //FIXME 这里使用Object.wait不能保证zk通知后于wait发生
                List<String> list = zk.getChildren(barrierPath, true);
                if (list.size() < size) {
                    mutex.wait();
                } else {
                    return;
                }
            }
        }
    }

    @Override
    public void leave() throws Exception {
        zk.delete(barrierPath + "/" + nodeName, -1);
        Object mutex = new Object();
        while (true) {
            synchronized (mutex) {
                List<String> list = zk.getChildren(barrierPath, true);
                if (list.size() > 0) {
                    mutex.wait();
                } else {
                    return;
                }
            }
        }
    }

    private void checkState() {
        switch(zkState) {
            case SyncConnected:
                return;
            case Expired:
                //create new client ?
            default:
                throw new IllegalStateException("zookeeper state : " + zkState);
        }
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                ZkBarrier.this.zkState = event.getState();
                cdl.countDown();
                break;
            case NodeChildrenChanged:
                synchronized (mutex) {
                    mutex.notify();
                }
                break;
            default:
                break;
        }
    }
}
