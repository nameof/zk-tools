package com.nameof.zookeeper.tools.common;

import com.google.common.base.Preconditions;
import com.nameof.zookeeper.tools.utils.ZkUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

import static org.apache.zookeeper.ZooKeeper.States.CONNECTED;

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

    public void destory() {
        boolean interrupt = false;
        try {
            do {
                try {
                    zk.close();
                    return;
                } catch (InterruptedException e) {
                    interrupt = true;
                }
            } while (true);
        } finally {
            if (interrupt) Thread.currentThread().interrupt();
        }
    }

    protected void checkState() {
        if (zk.getState() != CONNECTED)
            switch(zkState) {
                case Expired:
                    //create new client ?
                default:
                    throw new IllegalStateException("zookeeper state : " + zkState);
            }
    }
}
