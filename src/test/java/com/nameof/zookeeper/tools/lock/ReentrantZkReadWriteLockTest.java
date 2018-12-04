package com.nameof.zookeeper.tools.lock;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * @Author: chengpan
 * @Date: 2018/11/20
 */
public class ReentrantZkReadWriteLockTest {
    @Test
    public void testLockState() throws Exception {
        ReadWriteLock lock = new ReentrantZkReadWriteLock("l2", "172.16.98.129");
        Throwable t = null;
        try {
            lock.readLock().lock();
            lock.writeLock().lock();
        } catch (Exception e) {
            t = e;
        } finally {
            lock.readLock().unlock();
        }
        Assert.assertTrue(t instanceof IllegalStateException);
    }

    @Test
    public void testLock() throws Exception {
        final int concurrentSize = 10;
        ExecutorService es = Executors.newFixedThreadPool(concurrentSize);
        final CountDownLatch quit = new CountDownLatch(concurrentSize);
        for (int i = 0; i < concurrentSize; i++) {
            final int no = i;
            es.submit(()->{
                try {
                    ReadWriteLock lock = new ReentrantZkReadWriteLock("l2", "172.16.98.129");
                    Lock rl = no % 2 == 0 ? lock.readLock() : lock.writeLock();
                    String lockName = no % 2 == 0 ? "read" : "write";
                    rl.lock();
                    try {
                        System.out.println("get "+ lockName +  " lock-" + no);
                    } finally {
                        rl.unlock();
                        System.out.println("release " + lockName + " lock-" + no);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    quit.countDown();
                }
            });
        }
        quit.await();
    }
}
