package com.nameof.zookeeper.util.common;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 为实现基于zk的原语提供支持
 * @Author: chengpan
 * @Date: 2018/11/17
 */
public class ZkPrimitiveSupport {

    private final ZooKeeper zk;

    public ZkPrimitiveSupport(ZooKeeper zk) {
        this.zk = zk;
    }

    /**
     * 阻塞，等待path上的 NodeChildrenChanged 事件发生
     * @param phaser
     * @param path
     * @param duration
     * @throws InterruptedException
     * @throws TimeoutException
     */
    public void waitChildren(Phaser phaser, String path, WaitDuration duration) throws InterruptedException, TimeoutException {
        EventPhaserWatcher epw = new EventPhaserWatcher(Watcher.Event.EventType.NodeChildrenChanged, phaser);
        try {
            zk.getChildren(path, epw);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
        await(phaser, duration);
    }

    public void waitChildren(Phaser phaser, String path) throws InterruptedException {
        try {
            waitChildren(phaser, path, null);
        } catch (TimeoutException ignore) {
            //never happen
        }
    }

    public void waitNonChildren(String path) throws KeeperException, InterruptedException {
        Phaser phaser = new Phaser(1);
        while (true) {
            EventPhaserWatcher epw = new EventPhaserWatcher(Watcher.Event.EventType.NodeChildrenChanged, phaser);
            List<String> list = zk.getChildren(path, epw);
            if (list.size() > 0) {
                await(phaser);
            } else {
                return;
            }
        }
    }

    public void waitNotExists(Phaser phaser, String path) throws KeeperException, InterruptedException {
        try {
            waitNotExists(phaser, path, null);
        } catch (TimeoutException ignore) {
            //never happen
        }
    }

    public void waitNotExists(Phaser phaser, String path, WaitDuration duration) throws KeeperException, InterruptedException, TimeoutException {
        EventPhaserWatcher epw = new EventPhaserWatcher(Watcher.Event.EventType.NodeDeleted, phaser);
        Stat exists = zk.exists(path, epw);
        if (exists == null) {
            return;
        }
        await(phaser, duration);
    }

    private void await(Phaser phaser) throws InterruptedException {
        try {
            await(phaser, null);
        } catch (TimeoutException ignore) {
            //never happen
        }
    }

    private void await(Phaser phaser, WaitDuration duration) throws TimeoutException, InterruptedException {
        if (duration == null)
            phaser.awaitAdvance(phaser.getPhase());
        else
            phaser.awaitAdvanceInterruptibly(phaser.getPhase(), duration.getDuration(), duration.getUnit());
    }

    private static class EventPhaserWatcher implements Watcher {

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
