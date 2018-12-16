package org.fok.core.trie;

import java.util.concurrent.locks.Lock;

public class FokStorageTrieLock implements AutoCloseable {
    private final Lock lock;

    public FokStorageTrieLock(Lock l) {
        this.lock = l;
    }

    public final FokStorageTrieLock lock() {
        this.lock.lock();
        return this;
    }

    public final void close() {
        this.lock.unlock();
    }
}
