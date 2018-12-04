package com.nameof.zookeeper.tools.queue;

import com.google.common.collect.Lists;
import com.nameof.zookeeper.tools.common.ZkContext;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author chengpan
 */
public class ZkBlockingQueueTest {
    private BlockingQueue<Object> zkQueue;

    @Before
    public void before() throws InterruptedException, IOException, KeeperException {
        zkQueue = new ZkBlockingQueue("simple", "172.16.98.129", new Serializer() {
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

    @After
    public void after() {
        ((ZkContext)zkQueue).destory();
    }

    @Test
    public void testPoll() throws InterruptedException {
        zkQueue.clear();
        long l = System.currentTimeMillis();
        Object poll = zkQueue.poll(5, TimeUnit.SECONDS);
        Assert.assertEquals(null, poll);
        Assert.assertEquals(5, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - l));
    }

    @Test
    public void testDrainTo() throws InterruptedException {
        zkQueue.clear();
        zkQueue.add("1");
        zkQueue.add("2");
        ArrayList<Object> list = Lists.newArrayList();
        zkQueue.drainTo(list, 1);
        Assert.assertEquals(1, list.size());
    }

    @Test
    public void testTake() throws InterruptedException {
        System.out.println(zkQueue.take());
    }
}
