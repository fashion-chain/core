package org.fok.core.bean;

import java.util.List;

import lombok.Data;

@Data
public class BlockSyncMessage {
	long currentHeight;
	long wantHeight;
	private List<byte[]> syncTxHash;

	public enum BlockSyncCodeEnum {
		SS, ER, LB, LT
	}

	private BlockSyncCodeEnum syncCode;
}
