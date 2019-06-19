package org.fok.core.handler;

import java.util.concurrent.ExecutionException;

import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Provides;
import org.fok.core.api.ITransactionResultHandler;
import org.fok.core.datasource.FokTransactionDataAccess;
import org.fok.core.dbapi.ODBException;
import org.fok.core.model.Block.BlockInfo;
import org.fok.core.model.Transaction.TransactionInfo;

import com.google.protobuf.ByteString;

import lombok.extern.slf4j.Slf4j;
import onight.osgi.annotation.NActorProvider;
import onight.tfw.ntrans.api.ActorService;
import onight.tfw.ntrans.api.annotation.ActorRequire;

@NActorProvider
@Provides(specifications = { ActorService.class }, strategy = "SINGLETON")
@Instantiate(name = "core_transaction_handler")
@Slf4j
public class TransactionHandler implements ITransactionResultHandler, ActorService {
	@ActorRequire(name = "transaction_da", scope = "global")
	FokTransactionDataAccess transactionDA;

	@Override
	public void setTransactionDone(TransactionInfo tx, BlockInfo block, ByteString result) throws Exception {
		TransactionInfo.Builder oTransaction = tx.toBuilder();
		oTransaction.setStatus("D");
		oTransaction.setResult(result);
		transactionDA.saveTransaction(oTransaction.build());
	}

	@Override
	public void setTransactionError(TransactionInfo tx, BlockInfo block, ByteString result) throws Exception {
		TransactionInfo.Builder oTransaction = tx.toBuilder();
		oTransaction.setStatus("E");
		oTransaction.setResult(result);
		transactionDA.saveTransaction(oTransaction.build());
	}

}
