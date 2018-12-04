package com.nameof.zookeeper.tools.queue;/**
 * @Author: chengpan
 * @Date: 2018/11/8
 */

import com.nameof.zookeeper.tools.common.ZkContext;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Assert;
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
    private int size = 2;

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
        }, size);
    }

    @After
    public void after() {
        ((ZkContext)zkQueue).destory();
    }

    @Test
    public void testAdd() {
        zkQueue.clear();
        zkQueue.add("aaa");
        zkQueue.add("aaa");
        try {
            zkQueue.add("aaa");
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalStateException);
        }
    }
}
