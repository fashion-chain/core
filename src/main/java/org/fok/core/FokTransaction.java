package org.fok.core;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.StringUtils;
import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Provides;
import org.fok.actuator.impl.TransactionImpl;
import org.fok.core.api.ITransactionExecutorHandler;
import org.fok.core.bean.TransactionConfirmQueue;
import org.fok.core.bean.TransactionMessage;
import org.fok.core.bean.TransactionMessageQueue;
import org.fok.core.bean.TransactionStatus;
import org.fok.core.config.FokChainConfig;
import org.fok.core.config.FokChainConfigKeys;
import org.fok.core.cryptoapi.ICryptoHandler;
import org.fok.core.datasource.FokTransactionDataAccess;
import org.fok.core.exception.FokDposNodeNotReadyException;
import org.fok.core.exception.FokTransactionHandlerNotFoundException;
import org.fok.core.handler.AccountHandler;
import org.fok.core.handler.TransactionHandler;
import org.fok.core.model.Account.AccountInfo;
import org.fok.core.model.Block.BlockInfo;
import org.fok.core.model.Transaction.TransactionInfo;
import org.fok.core.model.Transaction.TransactionInput;
import org.fok.core.model.Transaction.TransactionNode;
import org.fok.core.model.Transaction.TransactionOutput;
import org.fok.tools.bytes.BytesComparisons;
import org.fok.tools.bytes.BytesHashMap;

import com.google.protobuf.ByteString;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import onight.osgi.annotation.NActorProvider;
import onight.tfw.ntrans.api.ActorService;
import onight.tfw.ntrans.api.annotation.ActorRequire;
import onight.tfw.outils.conf.PropHelper;

@NActorProvider
@Provides(specifications = { ActorService.class }, strategy = "SINGLETON")
@Instantiate(name = "fok_transaction_core")
@Slf4j
@Data
public class FokTransaction implements ActorService {
	@ActorRequire(name = "fok_block_chain_core", scope = "global")
	FokBlockChain blockChain;
	@ActorRequire(name = "fok_transaction_message_queue", scope = "global")
	TransactionMessageQueue tmMessageQueue;
	@ActorRequire(name = "fok_transaction_confirm_queue", scope = "global")
	TransactionConfirmQueue tmConfirmQueue;
	@ActorRequire(name = "fok_account_core", scope = "global")
	FokAccount account;
	@ActorRequire(name = "transaction_da", scope = "global")
	FokTransactionDataAccess transactionDA;
	@ActorRequire(name = "bc_crypto", scope = "global")
	ICryptoHandler crypto;
	@ActorRequire(name = "fok_chain_config", scope = "global")
	FokChainConfig chainConfig;
	@ActorRequire(name = "core_account_handler", scope = "global")
	AccountHandler oAccountHandler;
	@ActorRequire(name = "core_transaction_handler", scope = "global")
	TransactionHandler oTransactionHandler;

	public TransactionMessage createTransaction(TransactionInfo.Builder oTransactionInfo) throws Exception {
		if (!chainConfig.isNodeStart()) {
			throw new FokDposNodeNotReadyException("dpos node not ready");
		}
		TransactionNode.Builder oNode = TransactionNode.newBuilder();
		oNode.setAddress(ByteString.copyFrom(chainConfig.getNodeAddress()));
		oNode.setNid(chainConfig.getNodeId());
		oTransactionInfo.setNode(oNode);

		// BytesHashMap<AccountInfo.Builder> accounts =
		// getTransactionAccounts(oTransactionInfo.build());
		ITransactionExecutorHandler transactionExecutorHandler = TransactionImpl.getActuator(oAccountHandler,
				oTransactionHandler, crypto, oTransactionInfo.build(), blockChain.getLastConnectedBlock());
		if (transactionExecutorHandler.needSignature()) {
			transactionExecutorHandler.onVerifySignature(oTransactionInfo.build());
		}
		transactionExecutorHandler.onPrepareExecute(oTransactionInfo.build());
		oTransactionInfo.clearStatus();
		oTransactionInfo.clearHash();
		// 生成交易Hash
		oTransactionInfo.setHash(ByteString.copyFrom(crypto.sha256(oTransactionInfo.getBody().toByteArray())));

		if (transactionDA.isExistsTransaction(oTransactionInfo.getHash().toByteArray())) {
			throw new Exception("transaction exists, drop it txhash::" + oTransactionInfo.getHash());
		}

		TransactionMessage tm = new TransactionMessage(oTransactionInfo.getHash().toByteArray(),
				oTransactionInfo.build(), true);
		transactionDA.saveTransaction(tm.getTx());
		tmMessageQueue.addElement(tm);
		tmConfirmQueue.put(tm, BigInteger.ZERO);

		return tm;
	}

	public TransactionInfo getTransaction(byte[] txHash) throws Exception {
		return transactionDA.getTransaction(txHash);
	}

	public boolean isExistsTransaction(byte[] txHash) throws Exception {
		return transactionDA.getTransaction(txHash) != null;
	}

	public List<TransactionInfo> getWaitBlockTx(int count, int confirmTimes) {
		return tmConfirmQueue.poll(count, confirmTimes);
	}

	public boolean isExistsWaitBlockTx(byte[] hash) {
		return tmConfirmQueue.containsKey(hash);
	}

	public TransactionMessage removeWaitingSendOrBlockTx(byte[] txHash) throws Exception {
		TransactionMessage tmWaitBlock = tmConfirmQueue.invalidate(txHash);
		TransactionMessage tmWaitSend = tmMessageQueue.removeElement(txHash);
		if (tmWaitBlock != null && tmWaitBlock.getTx() != null) {
			return tmWaitBlock;
		} else {
			return tmWaitSend;
		}
	}

	public void setTransactionDone(TransactionInfo transaction, BlockInfo block, ByteString result) throws Exception {
		TransactionInfo.Builder oTransaction = transaction.toBuilder();
		oTransaction.setStatus("D");
		oTransaction.setResult(result);
		transactionDA.saveTransaction(oTransaction.build());
	}

	public boolean isDone(TransactionInfo transaction) {
		return "D".equals(transaction.getStatus());
	}

	public void setTransactionError(TransactionInfo transaction, BlockInfo block, ByteString result) throws Exception {
		TransactionInfo.Builder oTransaction = transaction.toBuilder();
		oTransaction.setStatus("E");
		oTransaction.setResult(result);
		transactionDA.saveTransaction(oTransaction.build());
	}

	public void syncTransaction(TransactionInfo transaction) {
		syncTransaction(transaction, true);
	}

	public void syncTransaction(TransactionInfo transaction, boolean isFromOther) {
		syncTransaction(transaction, true, new BigInteger("0"));
	}

	public void syncTransaction(TransactionInfo transaction, boolean isFromOther, BigInteger bits) {
		try {
			if (transactionDA.isExistsTransaction(transaction.getHash().toByteArray())) {
				log.warn("transaction " + crypto.bytesToHexStr(transaction.getHash().toByteArray())
						+ "exists in DB, drop it");
			} else {
				// BytesHashMap<AccountInfo.Builder> accounts =
				// getTransactionAccounts(transaction);
				ITransactionExecutorHandler transactionExecutorHandler = TransactionImpl.getActuator(oAccountHandler,
						oTransactionHandler, crypto, transaction, blockChain.getLastConnectedBlock());
				if (transactionExecutorHandler.needSignature()) {
					transactionExecutorHandler.onVerifySignature(transaction);
				}
				if (!BytesComparisons.equal(transaction.getHash().toByteArray(),
						crypto.sha3(transaction.getBody().toByteArray()))) {
					log.error("fail to sync transaction::" + crypto.bytesToHexStr(transaction.getHash().toByteArray())
							+ ", content invalid");
				} else {
					TransactionMessage tm = new TransactionMessage(transaction.getHash().toByteArray(), transaction,
							!isFromOther);
					transactionDA.saveTransaction(tm.getTx());
				}
			}
		} catch (Exception e) {
			log.error("fail to sync transaction::" + crypto.bytesToHexStr(transaction.getHash().toByteArray())
					+ " error::" + e, e);
		}
	}

	public void syncTransactionConfirm(byte[] hash, BigInteger bits) {
		try {
			// 如果交易已经确认过了，则直接返回，不再记录节点的确认次数
			TransactionInfo ti = getTransaction(hash);
			if (ti != null && ti.getBody() != null) {
				if (TransactionStatus.isDone(ti)) {
					return;
				}
			}
			tmConfirmQueue.increaseConfirm(hash, bits);
		} catch (Exception e) {
			log.error("");
		}
	}

	public void syncTransactionBatch(List<TransactionInfo> transactionInfos, BigInteger bits) throws Exception {
		syncTransactionBatch(transactionInfos, true, bits);
	}

	public void syncTransactionBatch(List<TransactionInfo> transactionInfos, boolean isFromOther, BigInteger bits) {
		if (transactionInfos.size() > 0) {
			List<byte[]> keys = new ArrayList<>();
			List<byte[]> values = new ArrayList<>();
			for (TransactionInfo transaction : transactionInfos) {
				try {
					TransactionInfo cacheTx = transactionDA.getTransaction(transaction.getHash().toByteArray());
					if (cacheTx == null) {
						// BytesHashMap<AccountInfo.Builder> accounts =
						// getTransactionAccounts(transaction);
						ITransactionExecutorHandler transactionExecutorHandler = TransactionImpl.getActuator(
								oAccountHandler, oTransactionHandler, crypto, transaction,
								blockChain.getLastConnectedBlock());
						if (transactionExecutorHandler.needSignature()) {
							transactionExecutorHandler.onVerifySignature(transaction);
						}
						TransactionMessage tm = new TransactionMessage(transaction.getHash().toByteArray(), transaction,
								false);
						if (isFromOther) {
							tmConfirmQueue.put(tm, bits);
						}
						transactionDA.saveTransaction(tm.getTx());
						keys.add(transaction.getHash().toByteArray());
						values.add(transaction.toByteArray());
					} else if (!isDone(cacheTx)) {
						tmConfirmQueue.increaseConfirm(transaction.getHash().toByteArray(), bits);
					}
				} catch (Exception e) {
					log.error("fail to sync transaction::" + transactionInfos.size() + " error::" + e, e);
				}
			}

			try {
				transactionDA.batchSaveTransaction(keys, values);
			} catch (Exception e) {
				log.error("fail to sync transaction::" + transactionInfos.size() + " error::" + e, e);
			}
		}
	}

	public byte[] getOriginalTransaction(TransactionInfo oTransaction) {
		TransactionInfo.Builder newTx = TransactionInfo.newBuilder();
		newTx.setBody(oTransaction.getBody());
		newTx.setHash(oTransaction.getHash());
		newTx.setNode(oTransaction.getNode());
		return newTx.build().toByteArray();
	}

//	public BytesHashMap<AccountInfo.Builder> getTransactionAccounts(TransactionInfo oTransactionInfo) {
//		BytesHashMap<AccountInfo.Builder> accounts = new BytesHashMap<>();
//		TransactionInput oInput = oTransactionInfo.getBody().getInput();
//		accounts.put(oInput.getAddress().toByteArray(), account.getAccountOrCreate(oInput.getAddress()));
//
//		for (TransactionOutput oOutput : oTransactionInfo.getBody().getOutputsList()) {
//			accounts.put(oOutput.getAddress().toByteArray(), account.getAccountOrCreate(oOutput.getAddress()));
//		}
//
//		if (StringUtils.isNotBlank(chainConfig.getTransaction_gas_address())) {
//			accounts.put(crypto.hexStrToBytes(chainConfig.getTransaction_gas_address()), account.getAccountOrCreate(
//					ByteString.copyFrom(crypto.hexStrToBytes(chainConfig.getTransaction_gas_address()))));
//		}
//		
//		// 加载所有的交易相关用户
//		return accounts;
//	}

	@AllArgsConstructor
	public class TransactionExecutor implements Runnable {
		LinkedBlockingQueue<TransactionInfo> queue;
		BlockInfo.Builder currentBlock;
		BytesHashMap<AccountInfo.Builder> accounts;
		Map<String, ByteString> results;
		CountDownLatch cdl;

		@Override
		public void run() {
			while (cdl.getCount() > 0) {
				TransactionInfo oTransaction = queue.poll();
				if (oTransaction != null) {
					ITransactionExecutorHandler transactionExecutorHandler = TransactionImpl.getActuator(
							oAccountHandler, oTransactionHandler, crypto, oTransaction, currentBlock.build());
					if (transactionExecutorHandler == null) {
						throw new FokTransactionHandlerNotFoundException("没有找到对应交易的执行器");
					}
					try {
						transactionExecutorHandler.onPrepareExecute(oTransaction);
						ByteString result = transactionExecutorHandler.onExecute(oTransaction);
						transactionExecutorHandler.onExecuteDone(oTransaction, currentBlock.build(), result);
						accounts.putAll(transactionExecutorHandler.getTouchAccount());
						results.put(crypto.bytesToHexStr(oTransaction.getHash().toByteArray()), result);
					} catch (Throwable e) {// e.printStackTrace();
						log.error("block " + crypto.bytesToHexStr(currentBlock.getHeader().getHash().toByteArray())
								+ " exec transaction hash::" + oTransaction.getHash() + " error::" + e.getMessage());
						try {
							transactionExecutorHandler.onExecuteError(oTransaction, currentBlock.build(), ByteString
									.copyFromUtf8(e.getMessage() == null ? "unknown exception" : e.getMessage()));
							results.put(crypto.bytesToHexStr(oTransaction.getHash().toByteArray()), ByteString
									.copyFromUtf8(e.getMessage() == null ? "unknown exception" : e.getMessage()));
						} catch (Exception e1) {
							log.error("onexec errro:" + e1.getMessage(), e1);
						}
					} finally {
						cdl.countDown();
					}
				}
			}
		}
	}
}
