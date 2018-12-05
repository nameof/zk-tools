package com.nameof.zookeeper.tools.barrier;

import com.nameof.zookeeper.tools.common.ZkContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @Author: chengpan
 * @Date: 2018/11/9
 */
public class ZkBarrierTest {
    @Test
    public void testEnter() throws InterruptedException {
        final int concurrentSize = 5;
        ExecutorService es = Executors.newFixedThreadPool(concurrentSize);
        final CountDownLatch quit = new CountDownLatch(concurrentSize);
        for (int i = 0; i < concurrentSize; i++) {
            final int no = i;
            es.submit(()-> {
                Barrier barrier = null;
                try {
                    barrier = new ZkBarrier("b12", "172.16.98.129", concurrentSize);
                    randomWaitAndEnter(barrier, no);
                    randomWaitAndLeave(barrier, no);
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                } finally {
                    if (barrier != null) ((ZkContext) barrier).destory();
                    quit.countDown();
                }
            });
        }
        quit.await();
    }

    private void randomWaitAndEnter(Barrier barrier, int no) throws Exception {
        TimeUnit.SECONDS.sleep(new Random(System.currentTimeMillis()).nextInt(2));
        barrier.enter();
        System.out.println("enter-" + no);
    }

    private void randomWaitAndLeave(Barrier barrier, int no) throws Exception {
        TimeUnit.SECONDS.sleep(new Random(System.currentTimeMillis()).nextInt(2));
        barrier.leave();
        System.out.println("leave-" + no);
    }
}
