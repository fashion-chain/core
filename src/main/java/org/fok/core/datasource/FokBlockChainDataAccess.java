package org.fok.core.datasource;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.felix.ipojo.annotations.Instantiate;
import org.fok.core.bean.BlockMessageBuffer;
import org.fok.core.config.FokChainConfig;
import org.fok.core.config.FokChainConfigKeys;
import org.fok.core.cryptoapi.ICryptoHandler;
import org.fok.core.dbapi.ODBException;
import org.fok.core.dbapi.ODBSupport;
import org.fok.core.model.Block.BlockInfo;
import org.fok.tools.bytes.BytesHashMap;
import org.fok.tools.bytes.BytesHelper;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;
import onight.osgi.annotation.NActorProvider;
import onight.tfw.ntrans.api.annotation.ActorRequire;
import onight.tfw.ojpa.api.DomainDaoSupport;
import onight.tfw.ojpa.api.annotations.StoreDAO;
import onight.tfw.outils.conf.PropHelper;

@NActorProvider
@Instantiate(name = "blockchain_da")
@Slf4j
@Data
public class FokBlockChainDataAccess extends SecondaryBaseDatabaseAccess {
	@ActorRequire(name = "fok_block_buffer", scope = "global")
	BlockMessageBuffer blockMessageBuffer;
	@StoreDAO(target = daoProviderId, daoClass = FokSecondaryDao.class)
	ODBSupport dao;
	@ActorRequire(name = "fok_chain_config", scope = "global")
	FokChainConfig chainConfig;
	PropHelper prop = new PropHelper(null);
	@ActorRequire(name = "bc_crypto", scope = "global")
	ICryptoHandler crypto;

	@ActorRequire(name = "common_da", scope = "global")
	FokCommonDataAccess fokCommonDA;

	@Override
	public String[] getCmds() {
		return new String[] { "BCDAO" };
	}

	@Override
	public String getModule() {
		return "CHAIN";
	}

	public void setDao(DomainDaoSupport dao) {
		this.dao = (ODBSupport) dao;
	}

	public ODBSupport getDao() {
		return dao;
	}

	protected Cache blockHashStorage;
	// TODO 不需要缓存根据高度查找Block的方法，因为没法判断缓存里是否包含完整的数据
	// protected Cache blockNumberStorage;
	protected static CacheManager cacheManager = CacheManager.create("./conf/ehcache.xml");

	public FokBlockChainDataAccess() {
		this.blockHashStorage = new Cache(
				prop.get(FokChainConfigKeys.block_message_storage_cache_nameId_key, "block_cache") + "_hash",
				prop.get(FokChainConfigKeys.block_message_storage_cache_size_key, 100), MemoryStoreEvictionPolicy.LRU,
				true, "./" + prop.get(FokChainConfigKeys.block_message_storage_cache_nameId_key, "block_cache"), true,
				0, 0, true, 120, null);
		cacheManager.addCache(this.blockHashStorage);
	}

	public void saveBlock(BlockInfo block) throws ODBException, InterruptedException, ExecutionException {
		put(dao, block.getHeader().getHash().toByteArray(), BytesHelper.longToBytes(block.getHeader().getHeight()),
				block.toByteArray());
		this.blockHashStorage.put(new Element(block.getHeader().getHash().toByteArray(), block));

		log.debug("区块存储   ===>   保存区块。 hash[{}] 高度[{}] ",
				crypto.bytesToHexStr(block.getHeader().getHash().toByteArray()), block.getHeader().getHeight());
	}

	public BlockInfo getBlockByHash(byte[] hash) throws Exception {
		Element element = this.blockHashStorage.get(hash);
		if (element != null) {
			return BlockInfo.parseFrom((byte[]) element.getObjectValue());
		}

		byte[] v = get(dao, hash);
		if (v != null) {
			BlockInfo block = BlockInfo.parseFrom(v);
			this.blockHashStorage.put(new Element(block.getHeader().getHash().toByteArray(), block));
			// TODO 不需要缓存根据高度查找Block的方法，因为没法判断缓存里是否包含完整的数据
			// Element e =
			// this.blockNumberStorage.get(block.getHeader().getHeight());
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

		log.debug("区块存储   ===>   没有找到hash为[{}]的区块。", crypto.bytesToHexStr(hash));
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

	public void connectBlock(BlockInfo block) throws Exception {
		fokCommonDA.setConnectBlockHash(block.getHeader().getHash().toByteArray());
	}

	public BlockInfo loadMaxConnectBlock() throws Exception {
		byte[] v = fokCommonDA.getConnectBlockHash();
		if (v != null) {
			return getBlockByHash(v);
		}
		return null;
	}

	public void stableBlock(BlockInfo block) throws Exception {
		fokCommonDA.setStableBlockHash(block.getHeader().getHash().toByteArray());
	}

	public BlockInfo loadMaxStableBlock() throws Exception {
		byte[] v = fokCommonDA.getStableBlockHash();
		if (v != null) {
			return getBlockByHash(v);
		}
		return null;
	}

	public BlockInfo loadGenesisBlock() throws Exception {
		return getBlockByHeight(0)[0];
	}
}
