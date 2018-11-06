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
                return obj.toString().getBytes();
            }

            @Override
            public Object deserialize(byte[] bytes) {
                return new String(bytes);
            }
        });
    }

    @Test
    public void testAdd() {
        System.out.println(zkQueue.size());
        zkQueue.add("a");
        System.out.println(zkQueue.size());
        System.out.println(zkQueue.offer("b"));
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

    @Test
    public void testToArray() {
        zkQueue.clear();
        zkQueue.add("avc");
        System.out.println(zkQueue.toArray()[0]);
        System.out.println(zkQueue.toArray(new String[0])[0]);
    }
}
