package com.nameof.zookeeper.util.barrier;

/**
 * @Author: chengpan
 * @Date: 2018/11/8
 */
public interface Barrier {
    void enter() throws Exception;
    void leave() throws Exception;
}
