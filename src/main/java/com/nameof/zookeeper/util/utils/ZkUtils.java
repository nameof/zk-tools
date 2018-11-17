package com.nameof.zookeeper.util.utils;

import com.google.common.collect.Lists;
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
        try {
            zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException ignore) { }
    }

    public static void crecatePersistSeq(ZooKeeper zk, String path, byte[] data) throws KeeperException, InterruptedException {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    }

    public static void deleteChildren(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        List<String> cs = zk.getChildren(path, false);
        for (String c: cs) {
            try {
                zk.delete(path + "/" + c, -1);
            } catch (KeeperException.NoNodeException ignore) { }
        }
    }

    public static Object getNodeData(ZooKeeper zk, String path, Serializer serializer) throws KeeperException, InterruptedException {
        byte[] data = zk.getData(path, false, null);
        if (data == null) return null;
        Object o = serializer.deserialize(data);
        return o;
    }

    public static Object getNodeDataWithDelete(ZooKeeper zk, String path, Serializer serializer) throws KeeperException, InterruptedException {
        Object o = getNodeData(zk, path, serializer);
        ZkUtils.deleteNode(zk, path);
        return o;
    }

    public static String getMinSeqChild(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        List<String> list = getSortedChildren(zk, path);
        return list.isEmpty() ? null : list.get(0);
    }

    public static boolean deleteNode(ZooKeeper zk, String s) throws KeeperException, InterruptedException {
        try {
            zk.delete(s, -1);
            return true;
        }catch (KeeperException.NoNodeException e) {
            return false;
        }
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
        List<String> list = getSortedChildren(zk, path);
        List<Object> objs = Lists.newArrayListWithCapacity(list.size());
        for (String s: list) {
            try {
                objs.add(getNodeData(zk, path + "/" + s, serializer));
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
        List<String> list = getSortedChildren(zk, path);
        List<Object> objs = Lists.newArrayListWithCapacity(list.size());
        for (String s: list) {
            try {
                String childPath = path + "/" + s;
                objs.add(getNodeDataWithDelete(zk, childPath, serializer));
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
        List<String> list = getSortedChildren(zk, path);
        List<Object> objs = Lists.newArrayListWithCapacity(maxElements);
        for (String s: list) {
            try {
                String childPath = path + "/" + s;
                objs.add(getNodeDataWithDelete(zk, childPath, serializer));
                if (--maxElements == 0)
                    break;
            } catch (KeeperException.NoNodeException e) {
                continue;
            }
        }
        return objs;
    }

    public static List<String> getSortedChildren(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        List<String> list = zk.getChildren(path, false);
        if (list.isEmpty()) return Collections.emptyList();
        Collections.sort(list);
        return list;
    }


    /**
     * 获取比指定节点小的前一个节点名，否则返回当前节点名
     * @param zk
     * @param path
     * @param nodeName
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static String getSortedPreviousNodeName(ZooKeeper zk, String path, String nodeName) throws KeeperException, InterruptedException {
        List<String> sortedChildren = getSortedChildren(zk, path);
        if (sortedChildren.isEmpty() || !sortedChildren.contains(nodeName)
                || nodeName.equals(sortedChildren.get(0)))
            return nodeName;

        int nodeIdx = 0;
        while (!nodeName.equals(sortedChildren.get(nodeIdx)))
            nodeIdx++;
        return sortedChildren.get(nodeIdx - 1);
    }
}
