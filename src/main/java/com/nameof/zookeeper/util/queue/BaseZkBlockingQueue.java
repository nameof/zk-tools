package com.nameof.zookeeper.util.queue;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public abstract class BaseZkBlockingQueue extends BaseQueue implements BlockingQueue<Object> {

    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }
}
