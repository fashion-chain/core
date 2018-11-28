package org.fok.core.datasource.bdb;

import org.brewchain.bcapi.backend.ODBDao;

import onight.tfw.ojpa.api.ServiceSpec;
import onight.tfw.outils.conf.PropHelper;

public class FokAccountDao extends ODBDao {

	public FokAccountDao(ServiceSpec serviceSpec) {
		super(serviceSpec);
	}

	@Override
	public String getDomainName() {
		return "account.." + new PropHelper(null).get("org.brewchain.account.slicecount", 16);
	}
}
