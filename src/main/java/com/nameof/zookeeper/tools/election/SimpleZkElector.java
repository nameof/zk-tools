package com.nameof.zookeeper.tools.election;

import com.nameof.zookeeper.tools.common.ZkContext;
import com.nameof.zookeeper.tools.utils.ZkUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * @Author: chengpan
 * @Date: 2018/11/22
 */
public class SimpleZkElector extends ZkContext implements ZkElector {

    private static final String NAMESPACE = "/zkelection";

    private ZkElectionListener listener;

    private String electionPath;
    private String nodeName;

    public SimpleZkElector(String serviceName, String connectString, ZkElectionListener electionListener) throws IOException, InterruptedException, KeeperException {
        super(connectString);
        init(serviceName, electionListener);
    }

    private void init(String serviceName, ZkElectionListener electionListener) throws KeeperException, InterruptedException {
        listener = new DelegateListener(electionListener);
        electionPath = NAMESPACE + "/" + serviceName;

        checkState();
        ZkUtils.createPersist(zk, NAMESPACE);
        ZkUtils.createPersist(zk, electionPath);
    }

    @Override
    public synchronized void joinElection() throws KeeperException {
        checkState();
        try {
            join();
            return;
        } catch (InterruptedException ignore) {
            quitElection();
            Thread.currentThread().interrupt();
        } catch (KeeperException e) {
            quitElection();
            throw e;
        }
    }

    private void join() throws KeeperException, InterruptedException {
        prepareElection();
        do {
            String precedNodeName = ZkUtils.getSortedPrecedNodeName(zk, electionPath, nodeName);
            if (precedNodeName.equals(nodeName)) {
                listener.onMaster();
                return;
            }
            listener.onSlave();
            if (standby(precedNodeName)) {
                break;
            }
        } while (true);
    }

    private void prepareElection() throws KeeperException, InterruptedException {
        if (nodeName == null) {
            nodeName = ZkUtils.createTempAndGetSeq(zk, electionPath + "/");
        }
    }

    private boolean standby(String precedNodeName) throws KeeperException, InterruptedException {
        String precedNodePath = electionPath + "/" + precedNodeName;
        Stat exists = zk.exists(precedNodePath, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    join();
                } catch (Exception e) {
                    listener.onError(e);
                }
            }
        });
        return exists != null;
    }

    @Override
    public synchronized void quitElection() throws KeeperException {
        checkState();
        quit();
        destory();
    }

    private void quit() throws KeeperException {
        if (nodeName != null) {
            ZkUtils.deleteNodeIgnoreInterrupt(zk, electionPath + "/" + nodeName);
            nodeName = null;
        }
    }

    private class DelegateListener implements ZkElectionListener {

        private final ZkElectionListener listener;

        public DelegateListener(ZkElectionListener listener) {
            this.listener = listener;
        }

        @Override
        public void onMaster() {
            try {
                listener.onMaster();
            } catch (Throwable e) {
                onError(e);
            }
        }

        @Override
        public void onSlave() {
            try {
                listener.onSlave();
            } catch (Throwable e) {
                onError(e);
            }
        }

        @Override
        public void onError(Throwable e) {
            try {
                listener.onError(e);
            } catch (Throwable ee) {
                logger.error("error on ZkElectionListener.onError()", ee);
            }
        }
    }
}
