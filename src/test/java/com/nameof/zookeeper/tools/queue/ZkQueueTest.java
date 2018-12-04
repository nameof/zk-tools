package com.nameof.zookeeper.tools.queue;

import com.nameof.zookeeper.tools.common.ZkContext;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Queue;

/**
 * @author chengpan
 */
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

    @After
    public void after() {
        ((ZkContext)zkQueue).destory();
    }

    @Test
    public void testRemove() {
        zkQueue.clear();
        zkQueue.add("testRemove");
        Assert.assertEquals(1, zkQueue.size());
        Assert.assertEquals("testRemove", zkQueue.element());
        Assert.assertEquals(1, zkQueue.size());
        Assert.assertEquals("testRemove", zkQueue.remove());
        Assert.assertEquals(0, zkQueue.size());
    }

    @Test
    public void testToArray() {
        zkQueue.clear();
        zkQueue.add("testToArray");
        Assert.assertEquals("testToArray", zkQueue.toArray()[0]);
        Assert.assertEquals("testToArray", zkQueue.toArray(new String[0])[0]);
    }
}
