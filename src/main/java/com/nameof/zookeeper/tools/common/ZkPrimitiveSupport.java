package com.nameof.zookeeper.tools.common;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.CountDownLatch;
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
     * @param path
     * @param duration
     * @throws InterruptedException
     * @throws TimeoutException
     */
    public void waitChildren(String path, WaitDuration duration) throws InterruptedException, TimeoutException {
        CountDownLatch cdl = new CountDownLatch(1);
        EventPhaserWatcher epw = new EventPhaserWatcher(Watcher.Event.EventType.NodeChildrenChanged, cdl);
        try {
            zk.getChildren(path, epw);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
        await(cdl, duration);
    }

    public void waitChildren(String path) throws InterruptedException {
        try {
            waitChildren(path, null);
        } catch (TimeoutException ignore) {
            //never happen
        }
    }

    public void waitNonChildren(String path) throws KeeperException, InterruptedException {
        while (true) {
            CountDownLatch cdl = new CountDownLatch(1);
            EventPhaserWatcher epw = new EventPhaserWatcher(Watcher.Event.EventType.NodeChildrenChanged, cdl);
            List<String> list = zk.getChildren(path, epw);
            if (list.size() > 0) {
                await(cdl);
            } else {
                return;
            }
        }
    }

    public void waitNotExists(String path) throws KeeperException, InterruptedException {
        try {
            waitNotExists(path, null);
        } catch (TimeoutException ignore) {
            //never happen
        }
    }

    public void waitNotExists(String path, WaitDuration duration) throws KeeperException, InterruptedException, TimeoutException {
        CountDownLatch cdl = new CountDownLatch(1);
        EventPhaserWatcher epw = new EventPhaserWatcher(Watcher.Event.EventType.NodeDeleted, cdl);
        Stat exists = zk.exists(path, epw);
        if (exists == null) {
            return;
        }
        await(cdl, duration);
    }

    private void await(CountDownLatch cdl) throws InterruptedException {
        try {
            await(cdl, null);
        } catch (TimeoutException e) {
            //never happen
        }
    }

    private void await(CountDownLatch cdl, WaitDuration duration) throws InterruptedException, TimeoutException {
        if (duration == null) {
            cdl.await();
        } else {
            if (duration.getDuration() <= 0)
                throw new TimeoutException();
            cdl.await(duration.getDuration(), duration.getUnit());
        }
    }

    private static class EventPhaserWatcher implements Watcher {

        private final Event.EventType type;
        private final CountDownLatch cdl;

        public EventPhaserWatcher(Event.EventType type, CountDownLatch cdl) {
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
