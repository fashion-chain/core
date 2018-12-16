package org.fok.core.trie;

import java.util.concurrent.locks.Lock;

public class FokTrieLocker implements AutoCloseable {
    private final Lock lock;

    public FokTrieLocker(Lock l) {
        this.lock = l;
    }

    public final FokTrieLocker lock() {
        this.lock.lock();
        return this;
    }

    public final void close() {
        this.lock.unlock();
    }
}
