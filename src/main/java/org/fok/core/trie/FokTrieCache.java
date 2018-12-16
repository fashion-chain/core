package org.fok.core.trie;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Provides;
import org.fok.tools.bytes.BytesHashMap;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import onight.osgi.annotation.NActorProvider;
import onight.tfw.ntrans.api.ActorService;

@NActorProvider
@Provides(specifications = { ActorService.class }, strategy = "SINGLETON")
@Slf4j
@Data
public class FokTrieCache implements ActorService {
	protected final BytesHashMap<byte[]> storage;

	protected ReadWriteLock rwLock = new ReentrantReadWriteLock();
	protected FokTrieLocker readLock = new FokTrieLocker(rwLock.readLock());
	protected FokTrieLocker writeLock = new FokTrieLocker(rwLock.writeLock());

	public FokTrieCache() {
		this(new BytesHashMap<byte[]>());
	}

	public FokTrieCache(BytesHashMap<byte[]> storage) {
		this.storage = storage;
	}

	public void put(byte[] key, byte[] val) {
		if (val == null) {
			delete(key);
		} else {
			try (FokTrieLocker l = writeLock.lock()) {
				storage.put(key, val);
			}
		}
	}

	public byte[] get(byte[] key) {
		try (FokTrieLocker l = readLock.lock()) {
			return storage.get(key);
		}
	}

	public void delete(byte[] key) {
		try (FokTrieLocker l = writeLock.lock()) {
			storage.remove(key);
		}
	}

	public Set<byte[]> keys() {
		try (FokTrieLocker l = readLock.lock()) {
			return getStorage().keySet();
		}
	}

	public void updateBatch(Map<byte[], byte[]> rows) {
		try (FokTrieLocker l = writeLock.lock()) {
			for (Map.Entry<byte[], byte[]> entry : rows.entrySet()) {
				put(entry.getKey(), entry.getValue());
			}
		}
	}

	public Map<byte[], byte[]> getStorage() {
		return storage;
	}
}
