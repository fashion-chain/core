package org.fok.core.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Provides;
import org.fok.core.config.FokChainConfig;
import org.fok.core.config.FokChainConfigKeys;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;
import onight.osgi.annotation.iPojoBean;
import onight.tfw.ntrans.api.ActorService;
import onight.tfw.ntrans.api.annotation.ActorRequire;
import onight.tfw.outils.conf.PropHelper;

@iPojoBean
@Provides(specifications = { ActorService.class }, strategy = "SINGLETON")
@Instantiate(name = "fok_transaction_message_queue")
@Data
@Slf4j
public class TransactionMessageQueue implements ActorService {
	protected Cache storage;
	protected static CacheManager cacheManager = CacheManager.create("./conf/ehcache.xml");
	protected final static String STR_COUNTER = "__idcounter";
	CounterData counter = new CounterData();
	static PropHelper prop = new PropHelper(null);

	public TransactionMessageQueue() {
		this(prop.get(FokChainConfigKeys.transaction_message_queue_cache_nameId_key, "transaction_message"),
				prop.get(FokChainConfigKeys.transaction_message_queue_cache_size_key, 1000));
	}

	public TransactionMessageQueue(String cacheName, int cacheSize) {
		this.storage = new Cache(cacheName, cacheSize, MemoryStoreEvictionPolicy.LRU, true, "./" + cacheName, true, 0,
				0, true, 120, null);
		cacheManager.addCache(this.storage);
		Element ele = this.storage.get(STR_COUNTER);
		if (ele != null && ele.getObjectValue() != null) {
			counter = (CounterData) ele.getObjectValue();
		}
	}

	public void shutdown() {
		storage.flush();
		cacheManager.shutdown();
	}

	public void addElement(TransactionMessage tm) {
		while (storage.putIfAbsent(new Element(counter.pending.incrementAndGet(), tm)) != null)
			;
	}

	public void addLast(TransactionMessage hp) {
		addElement(hp);
	}

	public TransactionMessage removeElement(byte[] txHash) {
		Element element = storage.get(txHash);
		if (element != null) {
			storage.remove(txHash);
			return (TransactionMessage) element.getObjectValue();
		} else {
			return null;
		}
	}

	public int size() {
		return (int) (counter.pending.get() - counter.sending.get());
	}

	public TransactionMessage pollFirst() {
		List<TransactionMessage> ret = poll(1);

		if (ret != null & ret.size() > 0) {
			return ret.get(0);
		}

		return null;
	}

	public synchronized List<TransactionMessage> poll(int size) {
		List<TransactionMessage> ret = new ArrayList<>();
		for (int i = 0; i < size && counter.sending.get() < counter.pending.get(); i++) {
			Element element = storage.get(counter.sending.incrementAndGet());
			if (element != null && element.getObjectValue() != null && element.getObjectValue() != null) {
				ret.add((TransactionMessage) element.getObjectValue());
			} else {
				log.debug("get empty sending:" + counter.sending.get() + ",p=" + counter.pending.get());
				counter.sending.decrementAndGet();
			}
		}
		if (counter.pending.get() > counter.saved.get()) {
			counter.saved.set(counter.pending.get() + 1000);
		}
		storage.put(new Element(STR_COUNTER, counter));
		storage.flush();
		return ret;

	}

	class CounterData implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		public AtomicLong pending = new AtomicLong(0);
		public AtomicLong sending = new AtomicLong(0);
		public AtomicLong saved = new AtomicLong(1000);

		public CounterData() {
		}

		public CounterData(AtomicLong pending, AtomicLong sending, AtomicLong saved) {
			this.pending = pending;
			this.sending = sending;
			this.saved = saved;
		}
	}
}
