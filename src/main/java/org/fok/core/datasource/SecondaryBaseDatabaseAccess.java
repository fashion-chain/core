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

public class SecondaryBaseDatabaseAccess extends BaseDatabaseAccess {

	protected byte[] put(ODBSupport dbs, byte[] key, byte[] secondaryKey, byte[] value)
			throws ODBException, InterruptedException, ExecutionException {
		return dbs.put(key, secondaryKey, value).get();
	}

	protected BytesHashMap<byte[]> getBySecondaryKey(ODBSupport dbs, byte[] secondaryKey)
			throws ODBException, InterruptedException, ExecutionException {
		return dbs.listBySecondKey(secondaryKey).get();
	}
}
