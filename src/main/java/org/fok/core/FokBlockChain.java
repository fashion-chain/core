package org.fok.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Provides;
import org.fok.core.bean.BlockMessage;
import org.fok.core.bean.BlockMessageBuffer;
import org.fok.core.bean.BlockMessageMark;
import org.fok.core.bean.BlockMessageMark.BlockMessageMarkEnum;
import org.fok.core.config.FokChainConfig;
import org.fok.core.crypto.model.KeyPairs;
import org.fok.core.cryptoapi.ICryptoHandler;
import org.fok.core.bean.BlockMessage.BlockMessageStatusEnum;
import org.fok.core.datasource.FokBlockChainDataAccess;
import org.fok.core.datasource.FokCommonDataAccess;
import org.fok.core.dbapi.ODBException;
import org.fok.core.exception.FokBlockChainCannotStartException;
import org.fok.core.exception.FokBlockChainConfigurationException;
import org.fok.core.exception.FokBlockNotFoundInBufferException;
import org.fok.core.exception.FokBlockRollbackNotAllowException;
import org.fok.core.model.Block.BlockInfo;
import org.fok.core.trie.FokStateTrie;
import org.fok.tools.bytes.BytesComparisons;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import onight.osgi.annotation.NActorProvider;
import onight.tfw.ntrans.api.ActorService;
import onight.tfw.ntrans.api.annotation.ActorRequire;

@NActorProvider
@Provides(specifications = { ActorService.class }, strategy = "SINGLETON")
@Instantiate(name = "fok_block_chain_core")
@Slf4j
@Data
public class FokBlockChain implements ActorService {
	@ActorRequire(name = "blockchain_da", scope = "global")
	FokBlockChainDataAccess blockChainDA;
	@ActorRequire(name = "common_da", scope = "global")
	FokCommonDataAccess commonDA;
	@ActorRequire(name = "fok_block_buffer", scope = "global")
	BlockMessageBuffer blockMessageBuffer;
	@ActorRequire(name = "bc_crypto", scope = "global")
	ICryptoHandler crypto;
	@ActorRequire(name = "fok_chain_config", scope = "global")
	FokChainConfig chainConfig;
	@ActorRequire(name = "fok_state_trie", scope = "global")
	FokStateTrie fokStateTrie;

	private long maxReceiveHeight = -1;
	private long maxConnectHeight = -1;
	private long maxStableHeight = -1;
	private BlockInfo maxReceiveBlock = null;
	private BlockInfo maxConnectBlock = null;
	private BlockInfo maxStableBlock = null;

	public synchronized void startBlockChain() {
		try {
			byte[] nodeAccountAddress = getNodeAccountAddress();
			if (nodeAccountAddress == null) {
				throw new Exception("cannot find node account");
			}
			chainConfig.setNodeAddress(nodeAccountAddress);
			log.debug("节点启动   ===>   设置节点账户地址[{}]", crypto.bytesToHexStr(nodeAccountAddress));

			BlockInfo maxStabledBlock = blockChainDA.loadMaxStableBlock();
			if (maxStabledBlock == null) {
				// 不可能找不到稳定状态的块，一旦找不到直接抛出异常
				throw new FokBlockChainCannotStartException("没有找到稳定状态的区块，区块链无法启动。");
			}
			maxStableHeight = maxStabledBlock.getHeader().getHeight();
			maxStableBlock = maxStabledBlock;
			log.debug("节点启动   ===>   稳定块: 高度[{%-10s}] hash[{%s}]", maxStableHeight,
					crypto.bytesToHexStr(maxStabledBlock.getHeader().getHash().toByteArray()));

			BlockInfo maxConnectedBlock = blockChainDA.loadMaxConnectBlock();
			if (maxConnectedBlock == null) {
				log.debug("节点启动   ===>   没有找到最高块。将使用稳定块代替最高块。");
				// 如果没有找到连接状态的块，则可能的情况是区块链第一次启动，只存在创世块。或者因为意外重启导致的数据丢失
				maxConnectedBlock = maxStabledBlock;
			}

			// 从最大的连接块开始，还原所有的连接状态的区块到buffer中，直到稳定块结束
			long maxBlockHeight = maxConnectedBlock.getHeader().getHeight();
			long minBlockHeight = maxBlockHeight - chainConfig.getBlock_stable_count();
			if (minBlockHeight < 0) {
				minBlockHeight = 0;
			}
			if (minBlockHeight < maxStabledBlock.getHeader().getHeight()) {
				minBlockHeight = maxStabledBlock.getHeader().getHeight();
			}

			if (maxBlockHeight == 0 && minBlockHeight == 0) {
				// 节点第一次启动，只存在创世块的情况
				maxStableHeight = maxStabledBlock.getHeader().getHeight();
				maxStableBlock = maxStabledBlock;
			} else {
				while (maxBlockHeight > minBlockHeight) {
					BlockInfo[] blocks = blockChainDA.getBlockByHeight(maxBlockHeight);
					for (BlockInfo block : blocks) {
						if (maxReceiveHeight < block.getHeader().getHeight()) {
							maxReceiveHeight = block.getHeader().getHeight();
							maxReceiveBlock = block;
						}
						if (maxConnectHeight < block.getHeader().getHeight()) {
							maxConnectHeight = block.getHeader().getHeight();
							maxConnectBlock = block;
						}
						BlockMessage bm = new BlockMessage(block.getHeader().getHash().toByteArray(),
								block.getHeader().getParentHash().toByteArray(), block.getHeader().getHeight(), block);
						bm.setStatus(BlockMessageStatusEnum.CONNECT);
						blockMessageBuffer.put(bm);

						log.debug(String.format("节点启动   ===>   连接区块。高度[{%-10s}] hash[{%s}]", bm.getHeight(),
								crypto.bytesToHexStr(bm.getHash())));
					}
					maxBlockHeight -= 1;
				}
			}
			chainConfig.setNodeStart(true);
			fokStateTrie.setRoot(maxConnectBlock.getHeader().getStateRoot().toByteArray());
			log.debug(String.format("节点启动   ===>   设置最高块 hash[%s] 高度[%-10s] 状态树[{}]",
					crypto.bytesToHexStr(maxConnectBlock.getHeader().getHash().toByteArray()),
					maxConnectBlock.getHeader().getHeight(),
					crypto.bytesToHexStr(maxConnectBlock.getHeader().getStateRoot().toByteArray())));
			log.debug("节点启动   ===>   节点启动");
		} catch (Exception e) {
			log.error("节点启动异常", e);
		}
	}

	public BlockInfo getLastConnectedBlock() {
		return maxConnectBlock;
	}
	
	public long getLastConnectedBlockHeight() {
		return maxConnectHeight;
	}

	public BlockInfo getLastStableBlock() {
		return maxStableBlock;
	}

	public BlockInfo getLastReceiveBlock() {
		return maxReceiveBlock;
	}

	public BlockInfo getBlockByHash(byte[] hash) {
		try {
			return blockChainDA.getBlockByHash(hash);
		} catch (Exception e) {
			log.error("根据hash获取区块异常 hash[{}]", crypto.bytesToHexStr(hash), e);
		}
		return null;
	}

	public BlockInfo getBlockByHeight(long height) {
		BlockInfo[] bi = listBlockByHeight(height);
		if (bi == null) {
			return null;
		} else if (bi.length == 0) {
			return null;
		}
		{
			return bi[0];
		}
	}

	public BlockInfo[] listBlockByHeight(long height) {
		try {
			return blockChainDA.getBlockByHeight(height);
		} catch (Exception e) {
			log.error("根据高度获取区块异常 高度[{}]", height, e);
		}
		return null;
	}

	public BlockMessageMark addBlock(BlockInfo block) throws Exception {
		BlockMessageMark oBlockMessageMark = new BlockMessageMark();

		if (maxReceiveHeight < block.getHeader().getHeight()) {
			maxReceiveHeight = block.getHeader().getHeight();
			maxReceiveBlock = block;
		}
		byte[] hash = block.getHeader().getHash().toByteArray();
		byte[] parentHash = block.getHeader().getParentHash().toByteArray();
		long height = block.getHeader().getHeight();

		BlockMessage bm = blockMessageBuffer.get(hash);
		if (bm != null && bm.getStatus().equals(BlockMessageStatusEnum.CONNECT)) {
			oBlockMessageMark.setMark(BlockMessageMarkEnum.DROP);
			return oBlockMessageMark;
		} else {
			if (maxStableHeight >= block.getHeader().getHeight()) {
				oBlockMessageMark.setMark(BlockMessageMarkEnum.DROP);
				return oBlockMessageMark;
			}
		}

		if (bm == null) {
			bm = new BlockMessage(hash, parentHash, height, block);
		}
		blockMessageBuffer.put(bm);
		blockChainDA.saveBlock(block);

		if (bm.getHeight() == 0) {
			// 如果是创世块，不放到buffer中，直接保存db，更新maxStableBlock
			if (maxConnectHeight < block.getHeader().getHeight()) {
				maxConnectHeight = block.getHeader().getHeight();
				maxConnectBlock = block;
			}
			if (maxStableHeight < block.getHeader().getHeight()) {
				maxStableHeight = block.getHeader().getHeight();
				maxStableBlock = block;
			}

			oBlockMessageMark.setMark(BlockMessageMarkEnum.APPLY);
		} else if (bm.getHeight() == 1) {
			// 如果是第一块，因为创世块不在buffer里，所以只要上一块是第0块都直接apply
			BlockInfo parentBlock = blockChainDA.getBlockByHash(parentHash);
			if (parentBlock.getHeader().getHeight() == 0) {
				oBlockMessageMark.setMark(BlockMessageMarkEnum.APPLY);
			} else {
				oBlockMessageMark.setMark(BlockMessageMarkEnum.ERROR);
			}
		} else {
			BlockMessage parentBM = blockMessageBuffer.get(parentHash);
			if (parentBM == null) {
				List<BlockMessage> prevHeightBlock = blockMessageBuffer.get(height - 1, true);
				if (prevHeightBlock.size() > 0) {
					// 如果存在上一个高度的block，但是hash与parentHash不一致，则继续请求上一个高度的block
					oBlockMessageMark.setMark(BlockMessageMarkEnum.EXISTS_PREV);
				} else if (BytesComparisons.equal(maxStableBlock.getHeader().getHash().toByteArray(),
						block.getHeader().getParentHash().toByteArray())
						&& maxStableBlock.getHeader().getHeight() == block.getHeader().getHeight() - 1) {
					// 如果上一个高度并且hash一致的block不在buffer里，而是稳定块（重启节点可能会发生）,则允许apply
					oBlockMessageMark.setMark(BlockMessageMarkEnum.APPLY);
				} else {
					// 如果找不到上一个block，则先缓存下来，等待上一个块同步过来后再次尝试apply
					oBlockMessageMark.setMark(BlockMessageMarkEnum.CACHE);
				}
			} else if (parentBM != null && !parentBM.getStatus().equals(BlockMessageStatusEnum.CONNECT)) {
				// 如果上一个block存在，但是还没有连接到链上，则先缓存当前节点，等待上一个块连接上之后再尝试apply
				oBlockMessageMark.setMark(BlockMessageMarkEnum.CACHE);
			} else {
				oBlockMessageMark.setMark(BlockMessageMarkEnum.APPLY);
			}
		}
		return oBlockMessageMark;
	}

	public BlockMessageMark tryAddBlock(BlockInfo block) {
		BlockMessageMark oBlockMessageMark = new BlockMessageMark();
		byte[] parentHash = block.getHeader().getParentHash().toByteArray();
		long height = block.getHeader().getHeight();

		BlockMessage parentBM = blockMessageBuffer.get(parentHash);
		if (parentBM == null) {
			List<BlockMessage> prevHeightBlock = blockMessageBuffer.get(height - 1, true);
			if (prevHeightBlock.size() > 0) {
				oBlockMessageMark.setMark(BlockMessageMarkEnum.EXISTS_PREV);
			} else {
				oBlockMessageMark.setMark(BlockMessageMarkEnum.DROP);
			}
		} else if (parentBM != null && !parentBM.getStatus().equals(BlockMessageStatusEnum.CONNECT)) {
			// 如果上一个block存在，但是还没有连接到链上，则先缓存当前节点，等待上一个块连接上之后再尝试apply
			oBlockMessageMark.setMark(BlockMessageMarkEnum.CACHE);
		} else if (parentBM != null && parentBM.getStatus().equals(BlockMessageStatusEnum.CONNECT)) {
			// 如果上一个block存在，并且已经链接上，则apply该block
			oBlockMessageMark.setMark(BlockMessageMarkEnum.APPLY);
		} else {
			oBlockMessageMark.setMark(BlockMessageMarkEnum.CACHE);
		}
		return oBlockMessageMark;
	}

	public BlockMessageMark tryConnectBlock(BlockInfo block) throws Exception {
		BlockMessageMark oBlockMessageMark = new BlockMessageMark();

		byte[] hash = block.getHeader().getHash().toByteArray();
		long height = block.getHeader().getHeight();

		if (height == 1) {
			// 如果block的高度为1，保存block到db中，buffer中设置block状态为已链接
			blockChainDA.saveBlock(block);
			blockChainDA.connectBlock(block);
			blockMessageBuffer.get(hash).setStatus(BlockMessageStatusEnum.CONNECT);

			if (maxConnectHeight < block.getHeader().getHeight()) {
				maxConnectHeight = block.getHeader().getHeight();
				maxConnectBlock = block;
			}

			oBlockMessageMark.setMark(BlockMessageMarkEnum.DONE);
			return oBlockMessageMark;
		} else {
			if (blockMessageBuffer.containsHash(hash)) {
				// 如果buffer中存在该block，则直接链接（如果块已经变的稳定，则会自动移出buffer）
				blockChainDA.connectBlock(block);
				blockMessageBuffer.get(hash).setStatus(BlockMessageStatusEnum.CONNECT);

				if (maxConnectHeight < block.getHeader().getHeight()) {
					maxConnectHeight = block.getHeader().getHeight();
					maxConnectBlock = block;
				}

				// 如果在buffer存在稳定的block，则设置该block为稳定状态，从buffer中移除
				int count = 0;
				BlockMessage stableBM = null;
				BlockMessage parentBM = blockMessageBuffer.get(block.getHeader().getParentHash().toByteArray());
				while (parentBM != null && count < chainConfig.block_stable_count) {
					count += 1;
					stableBM = parentBM;
					parentBM = blockMessageBuffer.get(parentBM.getParentHash());
				}
				if (stableBM != null && stableBM.getStatus().equals(BlockMessageStatusEnum.CONNECT)
						&& count >= chainConfig.getBlock_stable_count()) {
					tryStableBlock(stableBM.getBlock());
				}

				List<BlockMessage> childs = blockMessageBuffer.get(height + 1, false);
				if (childs.size() > 0) {
					oBlockMessageMark.getChildBlock().addAll(childs);
					oBlockMessageMark.setMark(BlockMessageMarkEnum.APPLY_CHILD);
				} else {
					oBlockMessageMark.setMark(BlockMessageMarkEnum.DONE);
				}
				return oBlockMessageMark;
			} else {
				throw new FokBlockNotFoundInBufferException(
						String.format("连接区块发生异常。在缓冲区中没有找到指定的区块。 高度[%-10s] hash[%s]", block.getHeader().getHeight(),
								crypto.bytesToHexStr(block.getHeader().getHash().toByteArray())));
			}
		}
	}

	public BlockMessageMark tryStableBlock(BlockInfo block) throws Exception {
		BlockMessageMark oBlockMessageMark = new BlockMessageMark();
		blockChainDA.stableBlock(block);
		blockMessageBuffer.remove(block.getHeader().getHash().toByteArray());

		if (maxConnectHeight < block.getHeader().getHeight()) {
			maxConnectHeight = block.getHeader().getHeight();
			maxConnectBlock = block;
		}
		if (maxStableHeight < block.getHeader().getHeight()) {
			maxStableHeight = block.getHeader().getHeight();
			maxStableBlock = block;
		}
		oBlockMessageMark.setMark(BlockMessageMarkEnum.DONE);
		return oBlockMessageMark;
	}

	/**
	 * 回滚区块链到指定的高度
	 * <p>
	 * 不允许回滚到安全块以内的高度
	 * 
	 * @param height
	 * @return
	 * @throws FokBlockRollbackNotAllowException
	 */
	public BlockInfo rollBackTo(long targetHeight) throws FokBlockRollbackNotAllowException {
		if (targetHeight > maxConnectHeight - chainConfig.getBlock_stable_count()) {

			BlockMessage bm = null;
			for (long i = maxConnectHeight; i >= targetHeight; i--) {
				if (i != 0) {
					List<BlockMessage> blocks = blockMessageBuffer.get(i);
					for (BlockMessage blockInBuffer : blocks) {
						if (blockInBuffer.getStatus().equals(BlockMessageStatusEnum.CONNECT)) {
							if (blockInBuffer.getHeight() != targetHeight) {
								blockInBuffer.setStatus(BlockMessageStatusEnum.UNKNOWN);
							}
							bm = blockInBuffer;
						}
					}
				} else {
					break;
				}
			}

			if (bm != null) {
				maxConnectHeight = bm.getHeight();
				maxConnectBlock = bm.getBlock();
				return bm.getBlock();
			} else {
				return null;
			}
		} else {
			throw new FokBlockRollbackNotAllowException(
					String.format("不允许回滚到不稳定区之外。 当前高度[%-10s] 稳定高度[%-10s] 回滚高度[%-10s]", maxConnectHeight,
							maxStableHeight, targetHeight));
		}
	}

	public void setNodeAccountAddress(byte[] address) throws Exception {
		byte[] existsAddress = commonDA.getNodeAccountAddress();
		if (existsAddress == null) {
			commonDA.setNodeAccountAddress(address);
		} else {
			throw new FokBlockChainConfigurationException(
					String.format("不允许重复设置节点账户。地址[%s]", crypto.bytesToHexStr(address)));
		}
	}

	public byte[] getNodeAccountAddress() {
		try {
			byte[] address = commonDA.getNodeAccountAddress();
			if (address == null) {
				if (chainConfig.isEnvironment_dev_mode()) {
					FileReader fr = null;
					BufferedReader br = null;
					try {
						fr = new FileReader("keystore" + File.separator + chainConfig.getEnvironment_net()
								+ File.separator + "keystore" + chainConfig.getEnvironment_dev_num() + ".json");
						br = new BufferedReader(fr);
						String keyStoreJsonStr = "";

						String line = br.readLine();
						while (line != null) {
							keyStoreJsonStr += line.trim().replace("\r", "").replace("\t", "");
							line = br.readLine();
						}
						br.close();
						fr.close();

						KeyPairs oAccountKeyPair = crypto.restoreKeyStore(keyStoreJsonStr,
								chainConfig.getEnvironment_mode_dev_pwd());
						if (oAccountKeyPair == null) {
							return null;
						} else {
							commonDA.setNodeAccountAddress(crypto.hexStrToBytes(oAccountKeyPair.getAddress()));
							return crypto.hexStrToBytes(oAccountKeyPair.getAddress());
						}
					} catch (Exception e) {
						if (br != null) {
							br.close();
						}
						if (fr != null) {
							fr.close();
						}
						throw e;
					}
				} else {
					return null;
				}
			} else {
				return address;
			}
		} catch (Exception e) {
			log.error("无法获取节点账户地址", e);
		}
		return null;
	}
}
