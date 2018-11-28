package org.fok.core.datasource;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Provides;
import org.fok.core.config.FokChainConfig;
import org.fok.core.dbapi.ODBException;
import org.fok.core.model.Transaction.TransactionInfo;

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
@Instantiate(name = "transaction_da")
@Slf4j
public class FokTransactionDataAccess extends BaseDatabaseAccess implements ActorService {
	@ActorRequire(name = "fok_chain_config", scope = "global")
	FokChainConfig chainConfig;

	protected Cache storage;
	protected final static CacheManager cacheManager = new CacheManager("./conf/ehcache.xml");

	public FokTransactionDataAccess() {
		this.storage = new Cache("pendingqueue_" + chainConfig.getTransaction_message_queue_cache_nameId(),
				chainConfig.getTransaction_message_queue_cache_size(), MemoryStoreEvictionPolicy.LRU, true,
				"./pendingcache_" + chainConfig.getTransaction_message_queue_cache_nameId(), true, 0, 0, true, 120,
				null);
		cacheManager.addCache(this.storage);
	}

	public void saveTransaction(TransactionInfo transaction)
			throws ODBException, InterruptedException, ExecutionException {

		put(dao, transaction.getTxHash().toByteArray(), transaction.toByteArray());
		Element element = new Element(transaction.getTxHash().toByteArray(), transaction.toByteArray());
		this.storage.put(element);
	}

	public void batchSaveTransaction(List<byte[]> keys, List<byte[]> values)
			throws ODBException, InterruptedException, ExecutionException {
		batchPuts(dao, keys, values);
	}

	public TransactionInfo getTransaction(byte[] txHash) throws Exception {
		Element element = this.storage.get(txHash);
		if (element != null && element.getObjectValue() != null) {
			return (TransactionInfo) element.getObjectValue();
		}

		byte[] v = get(dao, txHash);
		if (v != null) {
			return TransactionInfo.parseFrom(v);
		}

		return null;
	}

	public boolean isExistsTransaction(byte[] txHash) throws Exception {
		return getTransaction(txHash) != null;
	}
}
