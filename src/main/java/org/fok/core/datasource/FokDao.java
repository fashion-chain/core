package org.fok.core.datasource;

import org.fok.core.dbapi.ODBDao;

import onight.tfw.ojpa.api.ServiceSpec;
import onight.tfw.outils.conf.PropHelper;

public class FokDao extends ODBDao<byte[], byte[]> {

	public FokDao(ServiceSpec serviceSpec) {
		super(serviceSpec);
	}

	@Override
	public String getDomainName() {
		return "account.." + new PropHelper(null).get("org.brewchain.account.slicecount", 16);
	}
	
	
}
