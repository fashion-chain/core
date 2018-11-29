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

@NActorProvider
@Provides(specifications = { ActorService.class }, strategy = "SINGLETON")
@Instantiate(name = "fok_da")
@Slf4j
@Data
public abstract class BaseDatabaseAccess extends SessionModules<Message> {
	protected static final String daoProviderId = "fok_db";

	@StoreDAO(target = daoProviderId, daoClass = FokDao.class)
	ODBSupport<byte[], byte[]> dao;

	@Override
	public void onDaoServiceAllReady() {
		log.debug("service ready!!!!");
	}

	@Override
	public void onDaoServiceReady(DomainDaoSupport arg0) {
	}

	@SuppressWarnings("unchecked")
	public void setCommonDao(DomainDaoSupport dao) {
		this.dao = (ODBSupport<byte[], byte[]>) dao;
	}

	public ODBSupport<byte[], byte[]> getCommonDao() {
		return dao;
	}

	@Override
	public String[] getCmds() {
		return new String[] { "COREDAOS" };
	}

	@Override
	public String getModule() {
		return "CORE";
	}

	public boolean isReady() {
		if (dao != null && FokDao.class.isInstance(dao)) {
			return true;
		}
		return false;
	}

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
