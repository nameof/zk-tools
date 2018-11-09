package com.nameof.zookeeper.util.queue;/**
 * @Author: chengpan
 * @Date: 2018/11/8
 */

import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

/**
 * @Author: chengpan
 * @Date: 2018/11/8
 */
public class BoundedZkBlockingQueueTest {
    private BlockingQueue<Object> zkQueue;

    @Before
    public void before() throws InterruptedException, IOException, KeeperException {
        zkQueue = new BoundedZkBlockingQueue("simple", "172.16.98.129", new Serializer() {
            @Override
            public byte[] serialize(Object obj) {
                return obj.toString().getBytes();
            }

            @Override
            public Object deserialize(byte[] bytes) {
                return new String(bytes);
            }
        }, 1);
    }

    @Test
    public void testAdd() {
        System.out.println(zkQueue.add("aaa"));
        System.out.println(zkQueue.add("aaa"));
    }
}
