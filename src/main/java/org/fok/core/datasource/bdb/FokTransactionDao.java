package org.fok.core.datasource.bdb;

import org.brewchain.bcapi.backend.ODBDao;

import onight.tfw.ojpa.api.ServiceSpec;
import onight.tfw.outils.conf.PropHelper;

public class FokTransactionDao extends ODBDao {
	public FokTransactionDao(ServiceSpec serviceSpec) {
		super(serviceSpec);
	}
	@Override
	public String getDomainName() {
		return "tx.sec."+new PropHelper(null).get("org.brewchain.txsec.slicecount", 8);
	}
}
