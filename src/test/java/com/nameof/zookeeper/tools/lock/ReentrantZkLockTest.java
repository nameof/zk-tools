package com.nameof.zookeeper.tools.lock;

import com.nameof.zookeeper.tools.common.ZkContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * @Author: chengpan
 * @Date: 2018/11/16
 */
public class ReentrantZkLockTest {

    @Test
    public void testLockInterruptibly() throws Exception {
        Thread t = new Thread() {
            @Override
            public void run() {
                Lock lock = null;
                try {
                    lock = new ReentrantZkLock("l1", "172.16.98.129");
                    lock.lockInterruptibly();
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                } finally {
                    if (lock != null) {
                        lock.unlock();
                        ((ZkContext)lock).destory();
                    }
                }
            }
        };
        t.start();
        t.interrupt();
        t.join();
    }

    @Test
    public void testTryLockWait() throws Exception {
        Lock lock = new ReentrantZkLock("l1", "172.16.98.129");
        long start = System.currentTimeMillis();
        lock.tryLock(5, TimeUnit.SECONDS);
        try {
            Assert.assertEquals(5, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()-start));
        } finally {
            lock.unlock();
            ((ZkContext)lock).destory();
        }
    }

    @Test
    public void testTryLock() throws Exception {
        Lock lock = new ReentrantZkLock("l1", "172.16.98.129");
        System.out.println(lock.tryLock());
        lock.unlock();
        ((ZkContext)lock).destory();
    }

    @Test
    public void testLock() throws Exception {
        final int concurrentSize = 20;
        ExecutorService es = Executors.newFixedThreadPool(concurrentSize);
        final CountDownLatch quit = new CountDownLatch(concurrentSize);
        for (int i = 0; i < concurrentSize; i++) {
            final int no = i;
            es.submit(()->{
                try {
                    Lock lock = new ReentrantZkLock("l1", "172.16.98.129");
                    lock.lock();
                    try {
                        System.out.println("get lock-" + no);
                    } finally {
                        lock.unlock();
                        ((ZkContext)lock).destory();
                        System.out.println("release lock-" + no);
                    }
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                } finally {
                    quit.countDown();
                }
            });
        }
        quit.await();
    }
}
