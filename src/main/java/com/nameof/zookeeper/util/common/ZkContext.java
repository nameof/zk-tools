package com.nameof.zookeeper.util.common;

import com.google.common.base.Preconditions;
import com.nameof.zookeeper.util.utils.ZkUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @Author: chengpan
 * @Date: 2018/11/11
 */
public class ZkContext implements Watcher {

    protected ZooKeeper zk;

    protected volatile Watcher.Event.KeeperState zkState;

    public ZkContext(String connectString) throws IOException, InterruptedException {
        Preconditions.checkNotNull(connectString, "connectString null");
        zk = ZkUtils.createSync(connectString, this);
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.None)
            this.zkState = event.getState();
    }

    protected void checkState() {
        switch(zkState) {
            case SyncConnected:
                return;
            case Expired:
                //create new client ?
            default:
                throw new IllegalStateException("zookeeper state : " + zkState);
        }
    }

    protected static class EventLatchWatcher implements Watcher {

        private final Event.EventType type;
        private final CountDownLatch cdl;

        public EventLatchWatcher(Event.EventType type, CountDownLatch cdl) {
            this.type = type;
            this.cdl = cdl;
        }

        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == type)
                cdl.countDown();
        }
    }
}
