package org.fok.core.datasource;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.fok.core.dbapi.ODBException;
import org.fok.core.dbapi.ODBSupport;

import onight.tfw.ojpa.api.annotations.StoreDAO;

public class FokAccountDataAccess extends BaseDatabaseAccess {
	public byte[] getAccountFromDb(byte[] address) throws ODBException, InterruptedException, ExecutionException {
		return get(dao, address);
	}

	public byte[] getTrie(byte[] trieKey) throws ODBException, InterruptedException, ExecutionException {
		return get(dao, trieKey);
	}

	public byte[] putTrie(byte[] trieKey, byte[] trieValue)
			throws ODBException, InterruptedException, ExecutionException {
		return put(dao, trieKey, trieValue);
	}

	public void batchPutTrie(List<byte[]> keys, List<byte[]> values) throws ODBException, InterruptedException, ExecutionException {
		batchPuts(dao, keys, values);
	}
}
