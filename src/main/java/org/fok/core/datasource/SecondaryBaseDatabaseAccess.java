package org.fok.core.datasource;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Provides;
import org.fok.core.datasource.bdb.FokAccountDao;
import org.fok.core.datasource.bdb.FokBlockDao;
import org.fok.core.dbapi.ODBException;
import org.fok.core.dbapi.ODBSupport;
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
public abstract class SecondaryBaseDatabaseAccess extends BaseDatabaseAccess{	
	@StoreDAO(target = daoProviderId, daoClass = FokBlockDao.class)
	ODBSupport<byte[], byte[]> dao;
}
