package com.nameof.zookeeper.tools.election;

/**
 * @Author: chengpan
 * @Date: 2018/11/22
 */
public interface ZkElectionListener {
    void onMaster();

    /** 在系统整个运行期间，可能有多次master失败触发选举过程，则各节点可能会多次进入{@link @onSlave}状态 */
    void onSlave();

    void onError(Throwable e);
}
