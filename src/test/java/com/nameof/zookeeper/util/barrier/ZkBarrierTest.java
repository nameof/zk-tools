package com.nameof.zookeeper.util.barrier;

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
    public void testEnter() throws Exception {
        final int concurrentSize = 20;
        ExecutorService es = Executors.newFixedThreadPool(concurrentSize);
        final CountDownLatch quit = new CountDownLatch(concurrentSize);
        for (int i = 0; i < concurrentSize; i++) {
            final int no = i;
            es.submit(()->{
                try {
                    System.out.println("i'm in " + no);
                    Barrier barrier = new ZkBarrier("b12", "172.16.98.129", 2);
                    randomWaitAndEnter(barrier);
                    System.out.println("enter-" + no);
                    randomWaitAndLeave(barrier);
                    System.out.println("leave-" + no);
                    quit.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
            });
        }
        quit.await();
    }

    private void randomWaitAndEnter(Barrier barrier) throws Exception {
        TimeUnit.SECONDS.sleep(new Random(System.currentTimeMillis()).nextInt(2));
        barrier.enter();
    }

    private void randomWaitAndLeave(Barrier barrier) throws Exception {
        TimeUnit.SECONDS.sleep(new Random(System.currentTimeMillis()).nextInt(2));
        barrier.leave();
    }
}
