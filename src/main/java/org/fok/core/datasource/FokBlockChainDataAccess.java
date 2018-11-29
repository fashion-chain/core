package org.fok.core.datasource;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Provides;
import org.fok.core.bean.BlockMessageBuffer;
import org.fok.core.config.CommonConstant;
import org.fok.core.config.FokChainConfig;
import org.fok.core.dbapi.ODBException;
import org.fok.core.model.Block.BlockInfo;
import org.fok.core.dbmodel.Entity.SecondaryValue;
import org.fok.tools.bytes.BytesHashMap;
import org.fok.tools.bytes.BytesHelper;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;
import onight.osgi.annotation.NActorProvider;
import onight.tfw.ntrans.api.ActorService;
import onight.tfw.ntrans.api.annotation.ActorRequire;

@NActorProvider
@Provides(specifications = { ActorService.class }, strategy = "SINGLETON")
@Instantiate(name = "blockchain_da")
@Slf4j
@Data
public class FokBlockChainDataAccess extends SecondaryBaseDatabaseAccess {
	@ActorRequire(name = "fok_block_buffer", scope = "global")
	BlockMessageBuffer blockMessageBuffer;

	FokChainConfig chainConfig = new FokChainConfig();
	protected Cache blockHashStorage;
	// TODO 不需要缓存根据高度查找Block的方法，因为没法判断缓存里是否包含完整的数据
	// protected Cache blockNumberStorage;
	protected static CacheManager cacheManager = CacheManager.create("./conf/ehcache.xml");

	public FokBlockChainDataAccess() {
		this.blockHashStorage = new Cache(
				"pendingqueue_" + chainConfig.getBlock_message_storage_cache_nameId() + "_hash",
				chainConfig.getBlock_message_storage_cache_size(), MemoryStoreEvictionPolicy.LRU, true,
				"./pendingcache_" + chainConfig.getBlock_message_storage_cache_nameId(), true, 0, 0, true, 120, null);
		cacheManager.addCache(this.blockHashStorage);
		
	}

	public void saveBlock(BlockInfo block) throws ODBException, InterruptedException, ExecutionException {
		put(dao, block.getHeader().getHash().toByteArray(), block.toByteArray());
		this.blockHashStorage.put(new Element(block.getHeader().getHash().toByteArray(), block));
	}

	public BlockInfo getBlockByHash(byte[] hash) throws Exception {
		Element element = this.blockHashStorage.get(hash);
		if (element != null) {
			return BlockInfo.parseFrom((byte[]) element.getObjectValue());
		}

		byte[] v = get(dao, hash);
		if (v != null) {
			SecondaryValue oSV = SecondaryValue.parseFrom(v);
			BlockInfo block = BlockInfo.parseFrom(oSV.getData());
			this.blockHashStorage.put(new Element(block.getHeader().getHash().toByteArray(), block));
			// TODO 不需要缓存根据高度查找Block的方法，因为没法判断缓存里是否包含完整的数据
			// Element e = this.blockNumberStorage.get(block.getHeader().getHeight());
			// if (e == null) {
			// List<BlockInfo> li = new ArrayList<BlockInfo>();
			// li.add(block);
			// e = new Element(block.getHeader().getHeight(), li);
			// } else {
			// List<BlockInfo> li = (List<BlockInfo>) e.getObjectValue();
			// li.add(block);
			// e = new Element(e.getObjectKey(), li);
			// }
			// this.blockNumberStorage.put(e);
			return block;
		}
		return null;
	}

	public BlockInfo[] getBlockByHeight(long height) throws Exception {
		BytesHashMap<byte[]> blocks = getBySecondaryKey(dao, BytesHelper.longToBytes(height));
		BlockInfo[] bis = new BlockInfo[blocks.size()];
		int i = 0;
		for (Map.Entry<byte[], byte[]> entrySet : blocks.entrySet()) {
			bis[i] = BlockInfo.parseFrom(entrySet.getValue());
			i++;
		}
		return bis;
	}

	public BlockInfo loadMaxConnectBlock() throws Exception {
		byte[] v = get(dao, CommonConstant.Max_Connected_Block);
		if (v != null) {
			return getBlockByHash(v);
		}
		return null;
	}

	public BlockInfo loadMaxStableBlock() throws Exception {
		byte[] v = get(dao, CommonConstant.Max_Stabled_Block);
		if (v != null) {
			return getBlockByHash(v);
		}
		return null;
	}

	public BlockInfo loadGenesisBlock() throws Exception {
		return getBlockByHeight(0)[0];
	}
}
