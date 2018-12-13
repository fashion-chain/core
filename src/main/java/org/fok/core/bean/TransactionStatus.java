package org.fok.core.bean;

import org.fok.core.model.Transaction.TransactionInfo;
import org.fok.core.model.Transaction.TransactionInfoOrBuilder;

import com.google.protobuf.ByteString;

public class TransactionStatus {
	public static boolean isDone(TransactionInfoOrBuilder mtx) {
		return "D".equals(mtx.getStatus());
	}

	public static boolean isError(TransactionInfoOrBuilder mtx) {
		return "E".equals(mtx.getStatus());
	}

	public static boolean isProccessed(TransactionInfoOrBuilder mtx) {
		return isDone(mtx) || isError(mtx);
	}

	public static void setDone(TransactionInfo.Builder mtx) {
		setDone(mtx, ByteString.EMPTY);
	}

	public static void setDone(TransactionInfo.Builder mtx, ByteString result) {
		mtx.setStatus("D");
		mtx.setResult(result);
	}

	public static void setError(TransactionInfo.Builder mtx) {
		setError(mtx, ByteString.EMPTY);
	}

	public static void setError(TransactionInfo.Builder mtx, ByteString result) {
		mtx.setStatus("E");
		mtx.setResult(result);
	}
}
