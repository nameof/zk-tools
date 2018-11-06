package com.nameof.zookeeper.util.utils;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

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

    public static List<String> getChildren(ZooKeeper zk, String path) throws KeeperException, InterruptedException {
        return zk.getChildren(path, false);
    }

    public static String getMinSeqChildren(ZooKeeper zk, String prefix, String path) throws KeeperException, InterruptedException {
        List<String> list = zk.getChildren(path, false);
        if (!list.isEmpty()) {
            Integer min = new Integer(list.get(0).substring(prefix.length()));
            for(String s : list){
                Integer temp = new Integer(s.substring(7));
                if(temp < min) min = temp;
            }
            return min.toString();
        }
        return null;
    }
}
