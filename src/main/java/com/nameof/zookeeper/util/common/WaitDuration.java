package com.nameof.zookeeper.util.common;

import java.util.concurrent.TimeUnit;

/**
 * @Author: chengpan
 * @Date: 2018/11/22
 */
public class WaitDuration {

    private final long deadline;

    private final TimeUnit unit = TimeUnit.MILLISECONDS;

    private WaitDuration(long duration) {
        this.deadline = duration + System.currentTimeMillis();
    }

    public static WaitDuration from(long duration) {
        return new WaitDuration(duration);
    }

    public long getDuration() {
        long duration = deadline - System.currentTimeMillis();
        return duration >= 0 ? duration : 0;
    }

    public TimeUnit getUnit() {
        return unit;
    }
}
