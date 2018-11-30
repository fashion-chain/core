package org.fok.core.datasource;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Provides;
import org.fok.core.dbapi.ODBException;
import org.fok.core.dbapi.ODBSupport;
import org.fok.tools.bytes.BytesHashMap;

import com.google.protobuf.Message;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import onight.oapi.scala.commons.SessionModules;
import onight.osgi.annotation.NActorProvider;
import onight.tfw.ntrans.api.ActorService;
import onight.tfw.ojpa.api.DomainDaoSupport;
import onight.tfw.ojpa.api.annotations.StoreDAO;

public class BaseDatabaseAccess extends SessionModules<Message>{
	protected static final String daoProviderId = "fok_db";

	protected byte[] get(ODBSupport dbs, byte[] key)
			throws ODBException, InterruptedException, ExecutionException {
		return dbs.get(key).get();
	}

	protected byte[] put(ODBSupport dbs, byte[] key, byte[] value)
			throws ODBException, InterruptedException, ExecutionException {
		return dbs.put(key, value).get();
	}	

	protected byte[][] batchPuts(ODBSupport dbs, List<byte[]> keys, List<byte[]> values)
			throws ODBException, InterruptedException, ExecutionException {
		return dbs.batchPuts(keys, values).get();
	}
}
