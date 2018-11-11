package com.nameof.zookeeper.util.barrier;

import org.apache.zookeeper.KeeperException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @Author: chengpan
 * @Date: 2018/11/9
 */
public class ZkBarrierTest {

    private Barrier barrier;

    @Before
    public void before() throws InterruptedException, IOException, KeeperException {
        barrier = new ZkBarrier("b1", "172.16.98.129", 2);
    }

    @Test
    public void testEnter() throws Exception {
        barrier.enter();
    }
}
