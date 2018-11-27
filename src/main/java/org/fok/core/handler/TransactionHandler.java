package org.fok.core.handler;

import org.fok.core.api.ITransactionHandler;
import org.fok.core.model.Block.BlockInfo;
import org.fok.core.model.Transaction.TransactionInfo;

import com.google.protobuf.ByteString;

public class TransactionHandler implements ITransactionHandler {

	@Override
	public void setTransactionDone(TransactionInfo arg0, BlockInfo arg1, ByteString arg2) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setTransactionError(TransactionInfo arg0, BlockInfo arg1, ByteString arg2) {
		// TODO Auto-generated method stub

	}

}
