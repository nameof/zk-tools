package com.nameof.zookeeper.util.common;

import com.google.common.base.Preconditions;
import com.nameof.zookeeper.util.utils.ZkUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.Phaser;

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

    protected void checkState() {
        if (zk.getState() != CONNECTED)
            switch(zkState) {
                case Expired:
                    //create new client ?
                default:
                    throw new IllegalStateException("zookeeper state : " + zkState);
            }
    }

    protected static class EventPhaserWatcher implements Watcher {

        private final Event.EventType type;
        private final Phaser phaser;

        public EventPhaserWatcher(Event.EventType type, Phaser phaser) {
            this.type = type;
            this.phaser = phaser;
        }

        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == type)
                phaser.arrive();
        }
    }
}
