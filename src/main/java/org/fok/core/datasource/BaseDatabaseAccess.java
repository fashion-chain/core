package org.fok.core.datasource;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Provides;
import org.fok.core.dbapi.ODBException;
import org.fok.core.dbapi.ODBSupport;
import org.fok.tools.bytes.BytesHashMap;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import onight.osgi.annotation.NActorProvider;
import onight.tfw.ntrans.api.ActorService;
import onight.tfw.ojpa.api.annotations.StoreDAO;

@NActorProvider
@Provides(specifications = { ActorService.class }, strategy = "SINGLETON")
@Instantiate(name = "Fok_DatabaseAccess")
@Slf4j
@Data
public abstract class BaseDatabaseAccess implements ActorService {
	protected static final String daoProviderId = "fok_db";

	@StoreDAO(target = daoProviderId, daoClass = FokDao.class)
	ODBSupport<byte[], byte[]> dao;

	protected byte[] get(ODBSupport<byte[], byte[]> dbs, byte[] key)
			throws ODBException, InterruptedException, ExecutionException {
		return dbs.get(key).get();
	}

	protected byte[] put(ODBSupport<byte[], byte[]> dbs, byte[] key, byte[] value)
			throws ODBException, InterruptedException, ExecutionException {
		return dbs.put(key, value).get();
	}

	protected byte[][] batchPuts(ODBSupport<byte[], byte[]> dbs, List<byte[]> keys, List<byte[]> values)
			throws ODBException, InterruptedException, ExecutionException {
		return dbs.batchPuts(keys, values).get();
	}

	protected BytesHashMap<byte[]> getBySecondaryKey(ODBSupport<byte[], byte[]> dbs, byte[] secondaryKey)
			throws ODBException, InterruptedException, ExecutionException {
		return dbs.listBySecondKey(secondaryKey).get();
	}
}
