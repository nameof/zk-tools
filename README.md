# zk-tools
some tools based on zookeeper, queue、blocking queue、barriers、exclusive lock、readwrite lock. implement the JDK standard API

# usage
```
    mvn clean install -DskipTests
```
```
    <dependency>
        <groupId>com.nameof</groupId>
        <artifactId>zk-tools</artifactId>
        <version>1.0-SNAPSHOT</version>
    </dependency>
```

1.queue
```
    Queue<Object> zkQueue = new ZkQueue(queueName, connectString, mySerializer);
    zkQueue.offer(obj);
    zkQueue.add(obj);
    zkQueue.poll();
    //...
```

2.blocking queue
```
    BlockingQueue<Object> zkQueue = new ZkBlockingQueue(queueName, connectString, mySerializer);
    zkQueue.put(obj);
    zkQueue.take();
    //...
```

3.bounded blocking queue
```
    BlockingQueue<Object> zkQueue = new BoundedZkBlockingQueue(queueName, connectString, mySerializer, size);
    zkQueue.offer(obj);
    zkQueue.add(obj);
    //...
```

4.barrier
```
    Barrier barrier = new ZkBarrier(barrierName, connectString, size);
    barrier.enter();
    //...
    barrier.leave();
    //...
```

5.reentrant lock(fair)
```
    Lock lock = new ReentrantZkLock(lockName, connectString);
    lock.lock(); //tryLock(), lockInterruptibly(), tryLock(time, unit)
    try {
        //...
    } finally {
        lock.unlock();
    }
```

6.read write lock(fair, unsupport downgrading, upgrading, revocable)
```
    ReadWriteLock lock = new ReentrantZkReadWriteLock(lockName, connectString);
    Lock lockk = readMode ? lock.readLock() : lock.writeLock();
    lockk.lock(); //also tryLock(), lockInterruptibly(), tryLock(time, unit)
    try {
        //...
    } finally {
        lockk.unlock();
    }
```
