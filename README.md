# zk-util
some tool for zookeeper, queue、blocking queue、barriers、exclusive lock、readwrite lock

# usage
```
    mvn clean install -DskipTests
```
```
    <dependency>
        <groupId>com.nameof</groupId>
        <artifactId>zk-util</artifactId>
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
    zkQueue.offer(obj); //may return false
    zkQueue.add(obj); //may throw an IllegalStateException
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
