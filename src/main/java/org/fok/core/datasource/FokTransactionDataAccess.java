package org.fok.core.datasource;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Property;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Validate;
import org.fok.core.config.FokChainConfig;
import org.fok.core.config.FokChainConfigKeys;
import org.fok.core.dbapi.ODBException;
import org.fok.core.dbapi.ODBSupport;
import org.fok.core.model.Transaction.TransactionInfo;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;
import onight.osgi.annotation.NActorProvider;
import onight.tfw.ntrans.api.ActorService;
import onight.tfw.ntrans.api.annotation.ActorRequire;
import onight.tfw.ojpa.api.DomainDaoSupport;
import onight.tfw.ojpa.api.annotations.StoreDAO;
import onight.tfw.outils.conf.PropHelper;

@NActorProvider
@Provides(specifications = { ActorService.class }, strategy = "SINGLETON")
@Instantiate(name = "transaction_da")
@Slf4j
@Data
public class FokTransactionDataAccess extends BaseDatabaseAccess {
	@StoreDAO(target = daoProviderId, daoClass = FokDao.class)
	ODBSupport dao;
	@ActorRequire(name = "fok_chain_config", scope = "global")
	FokChainConfig chainConfig;
	protected Cache storage;
	protected static CacheManager cacheManager = CacheManager.create("./conf/ehcache.xml");
	PropHelper prop = new PropHelper(null);

	@Override
	public String[] getCmds() {
		return new String[] { "TRXDAO" };
	}

	@Override
	public String getModule() {
		return "CORE";
	}

	public void setDao(DomainDaoSupport dao) {
		this.dao = (ODBSupport) dao;
	}

	public ODBSupport getDao() {
		return dao;
	}

	public FokTransactionDataAccess() {
		this.storage = new Cache(
				"pendingqueue_"
						+ prop.get(FokChainConfigKeys.transaction_message_cache_nameId_key, "transaction_cache"),
				prop.get(FokChainConfigKeys.transaction_message_cache_size_key, 500), MemoryStoreEvictionPolicy.LRU,
				true,
				"./pendingcache_"
						+ prop.get(FokChainConfigKeys.transaction_message_cache_nameId_key, "transaction_cache"),
				true, 0, 0, true, 120, null);
		cacheManager.addCache(this.storage);
	}

	public void saveTransaction(TransactionInfo transaction)
			throws ODBException, InterruptedException, ExecutionException {

		put(dao, transaction.getHash().toByteArray(), transaction.toByteArray());
		Element element = new Element(transaction.getHash().toByteArray(), transaction.toByteArray());
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
