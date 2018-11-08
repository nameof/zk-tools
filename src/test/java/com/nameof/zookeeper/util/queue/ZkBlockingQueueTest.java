package com.nameof.zookeeper.util.queue;

import com.google.common.collect.Lists;
import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
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

    @Test
    public void testPoll() throws InterruptedException {
        long l = System.currentTimeMillis();
        Object poll = zkQueue.poll(100, TimeUnit.SECONDS);
        System.out.println("poll: " + poll);
        System.out.println(System.currentTimeMillis() - l);
    }

    @Test
    public void testDrainTo() throws InterruptedException {
        System.out.println(zkQueue.drainTo(Lists.newArrayList(), 10));
    }

    @Test
    public void testTake() throws InterruptedException {
        System.out.println(zkQueue.take());
    }
}
