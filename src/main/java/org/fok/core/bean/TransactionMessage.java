package org.fok.core.bean;

import java.io.Serializable;
import java.math.BigInteger;
import org.fok.core.model.Transaction.TransactionInfo;
import org.fok.tools.bytes.BytesComparisons;

import lombok.Data;

@Data
public class TransactionMessage implements Serializable {
	private static final long serialVersionUID = 5829951203336980748L;
	byte[] key;
	transient TransactionInfo tx;
	BigInteger bits = new BigInteger("0");
	boolean isRemoved = false;
	boolean isNeedBroadcast = false;
	boolean isStoredInDisk = false;
	long lastUpdateTime = System.currentTimeMillis();

	public TransactionMessage(byte[] key, TransactionInfo tx) {
		super();
		this.key = key;
		this.tx = tx;
		this.bits = BigInteger.ZERO;
	}

	public TransactionMessage(byte[] key, TransactionInfo tx, boolean isNeedBroadcast) {
		super();
		this.key = key;
		this.tx = tx;
		this.bits = BigInteger.ZERO;
		this.isNeedBroadcast = isNeedBroadcast;
	}

	@Override
	public int hashCode() {
		return key.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (obj instanceof TransactionMessage) {
			TransactionMessage tm = (TransactionMessage) obj;

			return BytesComparisons.equal(tm.getKey(), key);
		} else {
			return false;
		}
	}

	public synchronized void setBits(BigInteger bits) {
		this.bits = this.bits.or(bits);
	}
}
