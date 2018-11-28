package org.fok.core.datasource.bdb;

import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Validate;
import org.brewchain.bcapi.backend.ODBSupport;

import com.google.protobuf.Message;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import onight.oapi.scala.commons.SessionModules;
import onight.osgi.annotation.NActorProvider;
import onight.tfw.ojpa.api.DomainDaoSupport;
import onight.tfw.ojpa.api.annotations.StoreDAO;

@NActorProvider
@Data
@Slf4j
@Instantiate(name = "Fok_Daos")
public class FokDaos extends SessionModules<Message> {
	@StoreDAO(target = "bc_bdb", daoClass = FokAccountDao.class)
	ODBSupport accountDao;
	
	@StoreDAO(target = "bc_leveldb", daoClass = FokAccountDao.class)
	ODBSupport accountLevelDao;

	@StoreDAO(target = "bc_bdb", daoClass = FokCommonDao.class)
	ODBSupport commonDao;

	@StoreDAO(target = "bc_bdb", daoClass = FokTransactionDao.class)
	ODBSupport txsDao;

	@StoreDAO(target = "bc_bdb", daoClass = FokBlockDao.class)
	ODBSupport blockDao;

	@Override
	public void onDaoServiceAllReady() {
		log.debug("service ready!!!!");
	}

	@Override
	public void onDaoServiceReady(DomainDaoSupport arg0) {
	}

	public void setCommonDao(DomainDaoSupport commonDao) {
		this.commonDao = (ODBSupport) commonDao;
	}

	public ODBSupport getCommonDao() {
		return commonDao;
	}

	public void setAccountDao(DomainDaoSupport accountDao) {
		this.accountDao = (ODBSupport) accountDao;
	}

	public ODBSupport getAccountDao() {
		return accountDao;
	}

	public void setBlockDao(DomainDaoSupport blockDao) {
		this.blockDao = (ODBSupport) blockDao;
	}

	public ODBSupport getBlockDao() {
		return blockDao;
	}

	public void setTxsDao(DomainDaoSupport txsDao) {
		this.txsDao = (ODBSupport) txsDao;
	}

	public ODBSupport getTxsDao() {
		return txsDao;
	}

	@Override
	public String[] getCmds() {
		return new String[] { "FOKDAO" };
	}

	@Override
	public String getModule() {
		return "FokDaoModule";
	}

	@Validate
	public void init() {
		// new Thread(stats).start();
	}

	@Invalidate
	public void destroy() {
		// stats.running = false;
	}

	public boolean isReady() {
		if (blockDao != null && FokBlockDao.class.isInstance(blockDao) && blockDao.getDaosupport() != null
				&& txsDao != null && FokTransactionDao.class.isInstance(txsDao) && txsDao.getDaosupport() != null
				&& accountDao != null && FokAccountDao.class.isInstance(accountDao)
				&& accountDao.getDaosupport() != null) {
			return true;
		}
		return false;
	}

}
