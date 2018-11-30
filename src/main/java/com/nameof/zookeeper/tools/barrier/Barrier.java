package com.nameof.zookeeper.tools.barrier;

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
    void enter() throws Exception;

    /**
     *
     * @return
     * @throws Exception
     */
    void leave() throws Exception;
}
