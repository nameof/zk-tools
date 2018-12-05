package com.nameof.zookeeper.tools.election;

import org.apache.zookeeper.KeeperException;

/**
 * @Author: chengpan
 * @Date: 2018/11/22
 */
public interface ZkElector {

    void joinElection() throws KeeperException;

    void quitElection() throws KeeperException;
}
