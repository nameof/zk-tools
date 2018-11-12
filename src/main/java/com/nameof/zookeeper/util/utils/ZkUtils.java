package com.nameof.zookeeper.util.utils;

import com.google.common.collect.Lists;
import com.nameof.zookeeper.util.barrier.ZkBarrier;
import com.nameof.zookeeper.util.queue.Serializer;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZkUtils {

    public static ZooKeeper createSync(String zkQuorum, Watcher watcher) throws InterruptedException, IOException {
        CountDownLatch cdl = new CountDownLatch(1);
        ZooKeeper zk = new ZooKeeper(zkQuorum, 10_000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (watcher != null)
                    watcher.process(event);
                cdl.countDown();
            }
        });
        try {
            cdl.await();
        } catch (InterruptedException e) {
            zk.close();
            throw e;
        }
        return zk;
    }

    public static void createPersist(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        if (zk.exists(path, false) == null) {
            try {
                zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException ignore) { }
        }
    }

    public static void crecatePersistSeq(ZooKeeper zk, String path, byte[] data) throws KeeperException, InterruptedException {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    }

    public static void deleteChildren(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        List<String> cs = zk.getChildren(path, false);
        for (String c:
             cs) {
            try {
                zk.delete(path + "/" + c, -1);
            } catch (KeeperException.NoNodeException e) {
                e.printStackTrace();
            }
        }
    }

    public static String getMinSeqChildren(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        List<String> list = zk.getChildren(path, false);
        if (!list.isEmpty()) {
            Collections.sort(list);
            return list.get(0);
        }
        return null;
    }

    public static void delete(ZooKeeper zk, String s) throws KeeperException, InterruptedException {
        zk.delete(s, -1);
    }

    /**
     * 获取所有子节点数据，忽略被并发删除的节点
     * @param zk
     * @param path
     * @param serializer
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static List<Object> getAllChildrenData(ZooKeeper zk, String path, Serializer serializer) throws KeeperException, InterruptedException {
        List<String> list = zk.getChildren(path, false);
        if (list.isEmpty()) return Collections.emptyList();
        Collections.sort(list);
        List<Object> objs = Lists.newArrayListWithCapacity(list.size());
        for (String s: list) {
            try {
                byte[] data = zk.getData(path + "/" + s, false, null);
                objs.add(serializer.deserialize(data));
            } catch (KeeperException.NoNodeException e) {
                continue;
            }
        }
        return objs;
    }

    /**
     * 获取所有子节点数据，并移除子节点，忽略被并发删除的节点
     * 保证元素顺序为zookeeper元素字典序
     * @param zk
     * @param path
     * @param serializer
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static List<Object> takeAllChildrenData(ZooKeeper zk, String path, Serializer serializer) throws KeeperException, InterruptedException {
        List<String> list = zk.getChildren(path, false);
        if (list.isEmpty()) return Collections.emptyList();
        Collections.sort(list);
        List<Object> objs = Lists.newArrayListWithCapacity(list.size());
        for (String s: list) {
            try {
                String childPath = path + "/" + s;
                byte[] data = zk.getData(childPath, false, null);
                zk.delete(childPath, -1);
                objs.add(serializer.deserialize(data));
            } catch (KeeperException.NoNodeException e) {
                continue;
            }
        }
        return objs;
    }

    /**
     * 获取指定个数的子节点数据，并移除子节点，忽略被并发删除的节点
     * 保证元素顺序为zookeeper元素字典序
     * @param zk
     * @param path
     * @param serializer
     * @param maxElements
     * @return
     */
    public static List<Object> takeAllChildrenData(ZooKeeper zk, String path, Serializer serializer, int maxElements) throws KeeperException, InterruptedException {
        List<String> list = zk.getChildren(path, false);
        if (list.isEmpty()) return Collections.emptyList();
        Collections.sort(list);
        List<Object> objs = Lists.newArrayListWithCapacity(maxElements);
        for (String s: list) {
            try {
                String childPath = path + "/" + s;
                byte[] data = zk.getData(childPath, false, null);
                zk.delete(childPath, -1);
                objs.add(serializer.deserialize(data));
                if (--maxElements == 0)
                    break;
            } catch (KeeperException.NoNodeException e) {
                continue;
            }
        }
        return objs;
    }
}
