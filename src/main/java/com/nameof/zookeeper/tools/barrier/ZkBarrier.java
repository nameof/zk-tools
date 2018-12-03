package com.nameof.zookeeper.tools.barrier;

import com.google.common.base.Preconditions;
import com.nameof.zookeeper.tools.common.ZkContext;
import com.nameof.zookeeper.tools.common.ZkPrimitiveSupport;
import com.nameof.zookeeper.tools.utils.ZkUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <p>一次性的分布式栅栏
 * <p>zookeeper官方的Barrier代码示例存在几个BUG，可能导致客户端永久阻塞：
 *     <p>1) 使用Object.wait , notify机制唤醒客户端，https://issues.apache.org/jira/browse/ZOOKEEPER-3186
 *     <p>2) enter和leave的事件通知竞态产生的ABA问题，https://issues.apache.org/jira/browse/ZOOKEEPER-1011，通过ready节点解决
 * <p><p>thread-safe
 * @Author: chengpan
 * @Date: 2018/11/8
 */
public class ZkBarrier extends ZkContext implements Barrier, Watcher {
    private static final String NAMESPACE = "/zkbarrier";

    private ZkPrimitiveSupport zkPrimitiveSupport;

    private int size;

    private String barrierPath;

    private String nodeName = UUID.randomUUID().toString();

    private AtomicBoolean allReady = new AtomicBoolean(false);

    private boolean destory = false;

    public ZkBarrier(String barrierName, String connectString, int size) throws IOException, InterruptedException, KeeperException {
        super(connectString);
        checkArgs(barrierName, size);
        init(barrierName, size);
    }

    private void checkArgs(String barrierName, int size) {
        Preconditions.checkNotNull(barrierName, "barrierName null");
        Preconditions.checkArgument(!barrierName.contains("/"), "barrierName invalid");
        Preconditions.checkNotNull(size > 0, "size invalid");
    }

    private void init(String barrierName, int size) throws KeeperException, InterruptedException {
        this.barrierPath = NAMESPACE + "/" + barrierName;
        this.size = size;
        this.zkPrimitiveSupport = new ZkPrimitiveSupport(zk);

        checkState();
        ZkUtils.createPersist(zk, NAMESPACE);
        ZkUtils.createPersist(zk, barrierPath);
    }

    @Override
    public synchronized void enter() throws Exception {
        checkState();

        prepareEnter();

        if (ready()) return;

        waitOthersEnter();

        notifyOthers();
    }

    private void prepareEnter() throws KeeperException, InterruptedException {
        ZkUtils.createTemp(zk, barrierPath + "/" + nodeName);
    }

    private boolean ready() throws KeeperException, InterruptedException {
        return zk.exists(getBarrierReadyPath(), new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                allReady.set(true);
                synchronized (ZkBarrier.this) {
                    ZkBarrier.this.notifyAll();
                }
            }
        }) != null;
    }

    private void waitOthersEnter() throws InterruptedException, KeeperException {
        List<String> list = zk.getChildren(barrierPath, false);
        while (!allReady.get() && list.size() < size) {
            wait();
        }
    }

    private void notifyOthers() throws KeeperException, InterruptedException {
        ZkUtils.createPersist(zk, getBarrierReadyPath());
    }

    @Override
    public synchronized void leave() throws Exception {
        checkState();

        prepareLeave();

        waitOthersLeave();

        cleanup();
    }

    private void prepareLeave() throws KeeperException {
        ZkUtils.deleteNodeIgnoreInterrupt(zk, barrierPath + "/" + nodeName);
    }

    private void waitOthersLeave() throws KeeperException, InterruptedException {
        zkPrimitiveSupport.waitNonChildren(barrierPath);
    }

    private void cleanup() throws KeeperException {
        try {
            ZkUtils.deleteNodeIgnoreInterrupt(zk, getBarrierReadyPath());
        } finally {
            destory();
        }
    }

    private String getBarrierReadyPath() {
        return barrierPath + "_ready";
    }

    @Override
    protected void checkState() {
        if (destory)
            throw new IllegalStateException("barrier destoryed");
        super.checkState();
    }
}
