package com.nameof.zookeeper.tools.election;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author: chengpan
 * @Date: 2018/11/22
 */
public class SimpleZkElectorTest {
    @Test
    public void test() throws InterruptedException {
        final int concurrentSize = 3;
        ExecutorService es = Executors.newFixedThreadPool(concurrentSize);
        for (int i = 0; i < concurrentSize; i++) {
            final int no = i;
            es.submit(()->{
                try {
                    ZkElectionListener listener = createListener(no);
                    ZkElector ze = new SimpleZkElector("db", "172.16.98.129", listener);
                    ze.joinElection();
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                }
            });
        }
        Thread.sleep(10000);
    }

    private ZkElectionListener createListener(int no) {
        return new ZkElectionListener() {
            @Override
            public void onMaster() {
                System.out.println("master start working " + no);
            }

            @Override
            public void onSlave() {
                System.out.println("stand-by " + no);
            }

            @Override
            public void onError(Throwable e) {
                Assert.fail(e.getMessage());
            }
        };
    }
}
