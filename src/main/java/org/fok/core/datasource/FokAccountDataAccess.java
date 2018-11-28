package org.fok.core.datasource;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.fok.core.datasource.bdb.FokAccountDao;
import org.fok.core.dbapi.ODBException;
import org.fok.core.dbapi.ODBSupport;

import onight.tfw.ojpa.api.annotations.StoreDAO;

public class FokAccountDataAccess extends BaseDatabaseAccess {
	@StoreDAO(target = daoProviderId, daoClass = FokAccountDao.class)
	ODBSupport<byte[], byte[]> accountDao;

	public byte[] getAccountFromDb(byte[] address) throws ODBException, InterruptedException, ExecutionException {
		return get(accountDao, address);
	}

	public byte[] getTrie(byte[] trieKey) throws ODBException, InterruptedException, ExecutionException {
		return get(accountDao, trieKey);
	}

	public byte[] putTrie(byte[] trieKey, byte[] trieValue)
			throws ODBException, InterruptedException, ExecutionException {
		return put(accountDao, trieKey, trieValue);
	}

	public void batchPutTrie(List<byte[]> keys, List<byte[]> values) throws ODBException, InterruptedException, ExecutionException {
		batchPuts(accountDao, keys, values);
	}
}
