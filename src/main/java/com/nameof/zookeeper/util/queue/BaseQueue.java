package com.nameof.zookeeper.util.queue;

import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;

public abstract class BaseQueue implements Queue<Object> {

    public boolean contains(Object o) {
        throw new UnsupportedOperationException();
    }

    public boolean containsAll(Collection c) {
        throw new UnsupportedOperationException();
    }

    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    public Iterator iterator() {
        throw new UnsupportedOperationException();
    }

    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    public Object[] toArray(Object[] a) {
        throw new UnsupportedOperationException();
    }

    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }
}
