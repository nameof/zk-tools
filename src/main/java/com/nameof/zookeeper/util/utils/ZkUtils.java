package com.nameof.zookeeper.util.utils;

import com.google.common.collect.Lists;
import com.nameof.zookeeper.util.queue.Serializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.Collections;
import java.util.List;

public class ZkUtils {
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

    public static List<Object> getAllChildrenData(ZooKeeper zk, String path, Serializer serializer) throws KeeperException, InterruptedException {
        List<String> list = zk.getChildren(path, false);
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
}
