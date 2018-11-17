package com.nameof.zookeeper.util.barrier;

/**
 * @Author: chengpan
 * @Date: 2018/11/8
 */
public interface Barrier {
    /**
     *
     * @return
     * @throws Exception
     */
    boolean enter() throws Exception;

    /**
     *
     * @return
     * @throws Exception
     */
    boolean leave() throws Exception;
}
