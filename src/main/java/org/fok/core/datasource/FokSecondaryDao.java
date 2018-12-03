package org.fok.core.datasource;

import org.fok.core.dbapi.ODBDao;

import onight.tfw.ojpa.api.ServiceSpec;
import onight.tfw.outils.conf.PropHelper;

public class FokSecondaryDao extends ODBDao {

	public FokSecondaryDao(ServiceSpec serviceSpec) {
		super(serviceSpec);
	}

	@Override
	public String getDomainName() {
		return "bdb.index";
	}
}
