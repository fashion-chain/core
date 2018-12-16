package org.fok.core.trie;

import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Provides;
import org.fok.core.config.FokChainConfig;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;
import onight.osgi.annotation.NActorProvider;
import onight.tfw.ntrans.api.ActorService;
import onight.tfw.ntrans.api.annotation.ActorRequire;

@NActorProvider
@Provides(specifications = { ActorService.class }, strategy = "SINGLETON")
@Instantiate(name = "fok_storagetrie_cache")
@Slf4j
@Data
public class FokStorageTrieCache implements ActorService {
	protected Cache storage;
	protected static CacheManager cacheManager = CacheManager.create("./conf/ehcache.xml");
	FokChainConfig chainConfig = new FokChainConfig();

	public FokStorageTrieCache() {
		this.storage = new Cache(chainConfig.getAccount_storage_cache_nameId(),
				chainConfig.getAccount_storage_cache_size(), MemoryStoreEvictionPolicy.LRU, true,
				"./" + chainConfig.getAccount_storage_cache_nameId(), true, 0, 0, true, 120, null);
		cacheManager.addCache(this.storage);
	}

	public void put(byte[] key, FokStorageTrie val) {
		if (val == null) {
			delete(key);
		} else {
			Element e = new Element(key, val);
			storage.put(e);
		}
	}

	public FokStorageTrie get(byte[] key) {
		Element e = this.storage.get(key);
		if (e != null && e.getObjectValue() != null) {
			return (FokStorageTrie) e.getObjectValue();
		}
		return null;
	}

	public void delete(byte[] key) {
		storage.remove(key);
	}
	//
	// public Set<String> keys() {
	// try (FokStorageTrieLock l = readLock.lock()) {
	// return getStorage().asMap().keySet();
	// }
	// }
	//
	// public void updateBatch(Map<String, FokStorageTrie> rows) {
	// try (FokStorageTrieLock l = writeLock.lock()) {
	// for (Map.Entry<String, FokStorageTrie> entry : rows.entrySet()) {
	// put(entry.getKey(), entry.getValue());
	// }
	// }
	// }
	//
	// public Cache<String, FokStorageTrie> getStorage() {
	// return storage;
	// }
}
