package com.nameof.zookeeper.util.queue;

import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Queue;

public class ZkQueueTest {

    private Queue<Object> zkQueue;

    @Before
    public void before() throws InterruptedException, IOException, KeeperException {
        zkQueue = new ZkQueue("simple", "172.16.98.129", new Serializer() {
            @Override
            public byte[] serialize(Object obj) {
                return new byte[0];
            }

            @Override
            public Object deserialize(byte[] bytes) {
                return null;
            }
        });
    }

    @Test
    public void testAdd() {
        System.out.println(zkQueue.size());
        zkQueue.add(null);
        System.out.println(zkQueue.size());
        System.out.println(zkQueue.offer(null));
        System.out.println(zkQueue.size());
    }

    @Test
    public void testRemove() {
        System.out.println(zkQueue.size());
        System.out.println(zkQueue.element());
        System.out.println(zkQueue.size());
        System.out.println(zkQueue.remove());
        System.out.println(zkQueue.size());
    }
}
