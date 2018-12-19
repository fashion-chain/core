package org.fok.core.bean;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.fok.core.FokBlock;
import org.fok.core.FokTransaction;
import org.fok.core.model.Account.AccountInfo;
import org.fok.core.model.Transaction.TransactionInfo;
import org.fok.tools.bytes.BytesHashMap;

import com.google.protobuf.ByteString;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class ParalTransactionLoader implements Runnable {
	byte[] txHash;
	int dstIndex;
	CountDownLatch cdl;
	TransactionInfo[] bb;
	byte[][] txTrieBB;
	BytesHashMap<AccountInfo.Builder> accounts;
	ConcurrentLinkedQueue<byte[]> missingHash;
	long blocknumber;
	AtomicBoolean justCheck;
	FokTransaction fokTransaction;

	@Override
	public void run() {
		try {
			Thread.currentThread().setName("txloader-" + blocknumber);
			TransactionMessage tm = fokTransaction.removeWaitingSendOrBlockTx(txHash);
			TransactionInfo transactionInfo = null;
			if (tm != null) {
				transactionInfo = tm.getTx();
			}
			if (transactionInfo == null) {
				transactionInfo = fokTransaction.getTransaction(txHash);
			}
			if (transactionInfo == null || transactionInfo.getHash() == null
					|| transactionInfo.getHash().equals(ByteString.EMPTY)
					|| transactionInfo.getBody().getInputs() == null) {
				missingHash.add(txHash);
				justCheck.set(true);
			} else {
				if (!justCheck.get()) {
					bb[dstIndex] = transactionInfo;
					txTrieBB[dstIndex] = fokTransaction.getOriginalTransaction(transactionInfo);
					accounts.putAll(fokTransaction.getTransactionAccounts(transactionInfo));
				} else {
					log.error("cannot load tx accounts::txhash=" + txHash);
				}
			}

		} catch (Exception e) {
			log.error("error in loading tx:" + txHash + ",idx=" + dstIndex, e);
		} finally {
			cdl.countDown();
			Thread.currentThread().setName("statetrie-pool");
		}
	}

}
