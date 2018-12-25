package org.fok.core.bean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Provides;
import org.fok.core.bean.BlockMessage.BlockMessageStatusEnum;
import org.fok.tools.bytes.BytesComparisons;
import org.fok.tools.bytes.BytesHashMap;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import onight.osgi.annotation.NActorProvider;
import onight.tfw.ntrans.api.ActorService;

@NActorProvider
@Provides(specifications = { ActorService.class }, strategy = "SINGLETON")
@Instantiate(name = "fok_block_buffer")
@Slf4j
@Data
public class BlockMessageBuffer implements ActorService {
	BytesHashMap<BlockMessage> hashStorage = new BytesHashMap<>();
	HashMap<Long, List<BlockMessage>> heightStorage = new HashMap<>();

	public boolean containsHash(byte[] hash) {
		return hashStorage.containsKey(hash);
	}

	public boolean containsHeight(Long height) {
		return heightStorage.containsKey(height);
	}

	public BlockMessage get(byte[] hash) {
		return hashStorage.get(hash);
	}

	public List<BlockMessage> get(long height) {
		return heightStorage.get(height);
	}

	public List<BlockMessage> get(long height, boolean connected) {
		List<BlockMessage> all = heightStorage.get(height);
		List<BlockMessage> ret = new ArrayList<>();
		if (all == null) {
			return ret;
		}
		for (BlockMessage bm : all) {
			if (bm.getStatus().equals(BlockMessageStatusEnum.CONNECT)) {
				ret.add(bm);
			}
		}
		return ret;
	}

	public void put(BlockMessage bm) {
		hashStorage.put(bm.getHash(), bm);
		if (heightStorage.containsKey(bm.getHeight())) {
			heightStorage.get(bm.getHeight()).add(bm);
		} else {
			List<BlockMessage> ls = new ArrayList<>();
			ls.add(bm);
			heightStorage.put(Long.parseLong(String.valueOf(bm.getHeight())), ls);
		}
	}

	public void remove(byte[] hash) {
		if (hashStorage.containsKey(hash)) {
			BlockMessage bm = hashStorage.get(hash);
			hashStorage.remove(hash);
			Iterator<BlockMessage> it = heightStorage.get(bm.getHeight()).iterator();
			while (it.hasNext()) {
				BlockMessage itBM = it.next();
				if (BytesComparisons.equal(itBM.getHash(), hash)) {
					it.remove();
					break;
				}
			}
		}
	}

	public void remove(long height) {
		if (heightStorage.containsKey(height)) {
			List<BlockMessage> list = heightStorage.get(height);
			heightStorage.remove(height);
			for (BlockMessage bm : list) {
				hashStorage.remove(bm.getHash());
			}
		}
	}

	public void clear() {
		heightStorage.clear();
		hashStorage.clear();
	}
}
