package org.fok.core.datasource;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Provides;
import org.fok.core.dbapi.ODBException;
import org.fok.core.dbapi.ODBSupport;

import com.google.protobuf.Message;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import onight.oapi.scala.commons.SessionModules;
import onight.osgi.annotation.NActorProvider;
import onight.tfw.ntrans.api.ActorService;
import onight.tfw.ojpa.api.DomainDaoSupport;
import onight.tfw.ojpa.api.annotations.StoreDAO;

@NActorProvider
@Provides(specifications = { ActorService.class }, strategy = "SINGLETON")
@Instantiate(name = "account_da")
@Slf4j
@Data
public class FokAccountDataAccess extends BaseDatabaseAccess {
	@StoreDAO(target = daoProviderId, daoClass = FokDao.class)
	ODBSupport dao;
	
	@Override
	public String[] getCmds() {
		return new String[] { "ACTDAO" };
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
