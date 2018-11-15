# zk-util
some use cases for zookeeper, queue、blocking queue、barriers、exclusive lock、readwrite lock

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
3.barrier
```
    Barrier barrier = new ZkBarrier(barrierName, connectString, 1);
    barrier.enter();
    //...
    barrier.leave();
```
