package com.nameof.zookeeper.util.barrier;

/**
 * @Author: chengpan
 * @Date: 2018/11/8
 */
public interface Barrier {
    /**
     *
     * @return 重复enter则返回false
     * @throws Exception
     */
    boolean enter() throws Exception;

    /**
     *
     * @return 尚未enter或已leave则返回false
     * @throws Exception
     */
    boolean leave() throws Exception;
}
