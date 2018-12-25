package org.fok.core.bean;

import java.util.ArrayList;
import java.util.List;

import org.fok.core.model.Block.BlockInfo;

import lombok.Data;

@Data
public class BlockMessageMark {
	private BlockMessageMarkEnum mark;
	private BlockInfo block;
	private List<BlockMessage> childBlock = new ArrayList<>();

	public enum BlockMessageMarkEnum {
		DROP, EXISTS_DROP, EXISTS_PREV, CACHE, APPLY, APPLY_CHILD, STORE, DONE, ERROR, NEED_TRANSACTION
	}
}
