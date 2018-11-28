package org.fok.core.datasource.bdb;

import org.brewchain.bcapi.backend.ODBDao;

import onight.tfw.ojpa.api.ServiceSpec;
import onight.tfw.outils.conf.PropHelper;

public class FokBlockDao extends ODBDao {
	public FokBlockDao(ServiceSpec serviceSpec) {
		super(serviceSpec);
	}

	@Override
	public String getDomainName() {
		return "block.number."+new PropHelper(null).get("org.brewchain.block.slicecount", 4);
//		return "block.number";
	}
}
