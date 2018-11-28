package org.fok.core.datasource.bdb;

import org.brewchain.bcapi.backend.ODBDao;

import onight.tfw.ojpa.api.ServiceSpec;

public class FokCommonDao extends ODBDao{
	public FokCommonDao(ServiceSpec serviceSpec) {
		super(serviceSpec);
	}

	@Override
	public String getDomainName() {
		return "comm";
	}
}
