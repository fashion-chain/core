package org.fok.core;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Provides;
import org.fok.core.bean.BlockMessage;
import org.fok.core.bean.BlockMessageMark;
import org.fok.core.bean.BlockSyncMessage;
import org.fok.core.bean.BlockSyncMessage.BlockSyncCodeEnum;
import org.fok.core.bean.TransactionExecutorSeparator;
import org.fok.core.bean.TransactionMessage;
import org.fok.core.bean.BlockMessageMark.BlockMessageMarkEnum;
import org.fok.core.config.FokChainConfig;
import org.fok.core.cryptoapi.ICryptoHandler;
import org.fok.core.exception.FokTrieRuntimeException;
import org.fok.core.model.Account.AccountInfo;
import org.fok.core.model.Block.BlockBody;
import org.fok.core.model.Block.BlockHeader;
import org.fok.core.model.Block.BlockInfo;
import org.fok.core.model.Block.BlockMiner;
import org.fok.core.model.Transaction.TransactionInfo;
import org.fok.core.trie.FokCacheTrie;
import org.fok.core.trie.FokStateTrie;
import org.fok.tools.bytes.BytesComparisons;
import org.fok.tools.bytes.BytesHashMap;
import org.fok.tools.bytes.BytesHelper;
import org.fok.tools.rlp.FokRLP;
import org.fok.tools.thread.ThreadExecutorManager;
import org.fok.tools.thread.exception.ThreadPoolNotExistsException;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import onight.osgi.annotation.NActorProvider;
import onight.tfw.ntrans.api.ActorService;
import onight.tfw.ntrans.api.annotation.ActorRequire;

@NActorProvider
@Provides(specifications = { ActorService.class }, strategy = "SINGLETON")
@Instantiate(name = "fok_block_core")
@Slf4j
@Data
public class FokBlock implements ActorService {
	@ActorRequire(name = "fok_account_core", scope = "global")
	FokAccount fokAccount;

	@ActorRequire(name = "fok_transaction_core", scope = "global")
	FokTransaction fokTransaction;

	@ActorRequire(name = "fok_block_chain_core", scope = "global")
	FokBlockChain fokBlockChain;

	@ActorRequire(name = "bc_crypto", scope = "global")
	ICryptoHandler crypto;

	@ActorRequire(name = "fok_state_trie", scope = "global")
	FokStateTrie fokStateTrie;

	@ActorRequire(name = "fok_thread_manager", scope = "global")
	ThreadExecutorManager threadExecutorManager;

	@ActorRequire(name = "fok_chain_config", scope = "global")
	FokChainConfig chainConfig;

	private TransactionExecutorSeparator oTransactionExecutorSeparator = new TransactionExecutorSeparator();

	public FokBlock() {
		Thread.currentThread().setName("fok-block");
	}

	public BlockInfo createBlock(int minConfirm, byte[] extraData, String term) {
		// 如果不指定block的大小，默认只允许包含1000笔交易
		return createBlock(1000, minConfirm, extraData, term);
	}

	public BlockInfo createBlock(int containTxCount, int minConfirm, byte[] extraData, String term) {
		return createBlock(fokTransaction.getWaitBlockTx(containTxCount, minConfirm), extraData, term);
	}

	public BlockInfo createBlock(List<TransactionInfo> txs, byte[] extraData, String term) {
		Thread.currentThread().setName("fok-block-create");

		BlockInfo.Builder oBlockInfo = BlockInfo.newBuilder();
		BlockHeader.Builder oBlockHeader = oBlockInfo.getHeaderBuilder();
		BlockMiner.Builder oBlockMiner = oBlockInfo.getMinerBuilder();

		BlockInfo oBestBlockEntity = fokBlockChain.getLastConnectedBlock();
		if (oBestBlockEntity == null) {
			oBestBlockEntity = fokBlockChain.getLastStableBlock();
		}
		oBlockHeader.setParentHash(oBestBlockEntity.getHeader().getHash());

		long currentTimestamp = System.currentTimeMillis();
		oBlockHeader.setTimestamp(System.currentTimeMillis() == oBestBlockEntity.getHeader().getTimestamp()
				? oBestBlockEntity.getHeader().getTimestamp() + 1 : currentTimestamp);
		oBlockHeader.setHeight(oBestBlockEntity.getHeader().getHeight() + 1);

		if (extraData != null) {
			oBlockHeader.setExtraData(ByteString.copyFrom(extraData));
		}
		for (int i = 0; i < txs.size(); i++) {
			oBlockHeader.addTxHashs(txs.get(i).getHash());
		}
		oBlockMiner.setNid(chainConfig.getNodeId());
		oBlockMiner.setAddress(ByteString.copyFrom(chainConfig.getNodeAddress()));
		oBlockMiner.setReward(ByteString.copyFrom(BytesHelper.intToBytes(chainConfig.getBlock_reward())));

		oBlockInfo.setHeader(oBlockHeader);
		oBlockInfo.setMiner(oBlockMiner);
		oBlockInfo.setVersion(chainConfig.getBlock_version());

		try {
			processBlock(oBlockInfo, oBestBlockEntity);
			byte[] blockContent = BytesHelper.appendBytes(oBlockHeader.clearHash().build().toByteArray(),
					oBlockMiner.build().toByteArray());
			oBlockInfo.getHeaderBuilder().setHash(ByteString.copyFrom(crypto.sha256(blockContent)));
			BlockInfo newBlock = oBlockInfo.build();
			BlockMessageMark addMark = fokBlockChain.addBlock(newBlock);
			switch (addMark.getMark()) {
			case APPLY: {
				BlockMessageMark connectMark = fokBlockChain.tryConnectBlock(newBlock);
				log.debug("创建区块   ===>   hash[{}->{}] 高度[{}] 交易数量[{}] 状态[{}]",
						crypto.bytesToHexStr(oBlockInfo.getHeader().getHash().toByteArray()),
						crypto.bytesToHexStr(oBlockInfo.getHeader().getParentHash().toByteArray()),
						oBlockInfo.getHeader().getHeight(), oBlockInfo.getHeader().getTxHashsCount(),
						connectMark.getMark());

				if (connectMark.getMark().equals(BlockMessageMarkEnum.DONE)) {
					return newBlock;
				} else {
					return null;
				}
			}
			default:
				return null;
			}
		} catch (Exception e) {
			log.error("创建区块发生异常。hash[{}] 高度[{}] 交易数量[{}]",
					crypto.bytesToHexStr((oBlockHeader == null || oBlockHeader.getHash() == null)
							? BytesHelper.ZERO_BYTE_ARRAY : oBlockHeader.getHash().toByteArray()),
					oBestBlockEntity.getHeader().getHeight() + 1, (txs == null ? 0 : txs.size()), e);
		}
		return null;
	}

	public BlockInfo createGenesisBlock() throws Exception {
		if (fokBlockChain.getBlockByHeight(0) != null) {
			return null;
		} else {
			BlockInfo.Builder oBlockInfo = BlockInfo.newBuilder();
			BlockHeader.Builder oBlockHeader = oBlockInfo.getHeaderBuilder();

			oBlockHeader.setParentHash(ByteString.copyFrom(BytesHelper.EMPTY_BYTE_ARRAY));

			long currentTimestamp = System.currentTimeMillis();
			oBlockHeader.setTimestamp(currentTimestamp);
			oBlockHeader.setHeight(0);
			oBlockHeader.setTransactionRoot(ByteString.copyFrom(BytesHelper.EMPTY_BYTE_ARRAY));
			oBlockHeader.setStateRoot(ByteString.copyFrom(this.fokStateTrie.getRootHash()));
			oBlockHeader.setReceiptRoot(ByteString.copyFrom(BytesHelper.EMPTY_BYTE_ARRAY));
			oBlockHeader.setHash(ByteString.copyFrom(crypto.sha256(oBlockHeader.build().toByteArray())));

			fokBlockChain.addBlock(oBlockInfo.build());
			fokBlockChain.tryConnectBlock(oBlockInfo.build());
			fokBlockChain.tryStableBlock(oBlockInfo.build());
			return oBlockInfo.build();
		}
	}

	public BlockSyncMessage syncBlock(ByteString bs) {
		return syncBlock(bs, false);
	}

	public BlockSyncMessage syncBlock(ByteString bs, boolean fastSync) {
		BlockInfo.Builder block;
		try {
			block = BlockInfo.newBuilder().mergeFrom(bs);
			return syncBlock(block, false);
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new BlockSyncMessage();
	}

	public BlockSyncMessage syncBlock(BlockInfo.Builder block) {
		return syncBlock(block, false);
	}

	public BlockSyncMessage syncBlock(BlockInfo.Builder block, boolean fastSync) {
		log.info(String.format("同步区块   ===>   高度[%-10s] hash[%s] 交易数量[%-5s] pHash[%s]", block.getHeader().getHeight(),
				crypto.bytesToHexStr(block.getHeader().getHash().toByteArray()), block.getHeader().getTxHashsCount(),
				crypto.bytesToHexStr(block.getHeader().getParentHash().toByteArray())));
		BlockSyncMessage bsm = new BlockSyncMessage();

		try {
			BlockHeader.Builder oBlockHeader = block.getHeader().toBuilder().clone();
			oBlockHeader.clearHash();

			byte[] blockContent = BytesHelper.appendBytes(oBlockHeader.build().toByteArray(),
					block.getMiner().toByteArray());

			if (!BytesComparisons.equal(block.getHeader().getHash().toByteArray(), crypto.sha256(blockContent))) {
				log.error("同步区块   ===>   区块hash校验失败。原始hash[{}] 校验hash[{}]",
						crypto.bytesToHexStr(block.getHeader().getHash().toByteArray()),
						crypto.bytesToHexStr(crypto.sha256(blockContent)));

			} else {
				BlockMessageMark bmm = fokBlockChain.addBlock(block.build());
				while (bmm.getMark() != BlockMessageMarkEnum.DONE) {
					log.debug(String.format("同步区块   ===>   高度[%-10s] hash[%s] 状态[%s]", block.getHeader().getHeight(),
							crypto.bytesToHexStr(block.getHeader().getHash().toByteArray()), bmm.getMark()));

					switch (bmm.getMark()) {
					case DROP:
						// log.warn(String.format("同步区块 ===> 高度[%-10s] hash[%s]
						// 状态[%s]",
						// block.getHeader().getHeight(),
						// crypto.bytesToHexStr(block.getHeader().getHash().toByteArray()),
						// bmm.getMark()));
						bmm.setMark(BlockMessageMarkEnum.DONE);
						break;
					case EXISTS_DROP:
						// log.warn(String.format("同步区块 ===> 高度[%-10s] hash[%s]
						// 状态[%s]",
						// block.getHeader().getHeight(),
						// crypto.bytesToHexStr(block.getHeader().getHash().toByteArray()),
						// bmm.getMark()));
						bmm.setMark(BlockMessageMarkEnum.DONE);
						// oBlockStoreSummary.setBehavior(blockChainHelper.tryAddBlock(applyBlock.build()).getBehavior());
						break;
					case EXISTS_PREV:
						// log.warn(String.format("同步区块 ===> 高度[%-10s] hash[%s]
						// 状态[%s]",
						// block.getHeader().getHeight(),
						// crypto.bytesToHexStr(block.getHeader().getHash().toByteArray()),
						// bmm.getMark()));
						try {

							long rollBackNumber = block.getHeader().getHeight() - 2;
							fokBlockChain.rollBackTo(rollBackNumber);
							bsm.setSyncCode(BlockSyncCodeEnum.LB);
							bsm.setCurrentHeight(rollBackNumber);
							bsm.setWantHeight(rollBackNumber + 1);
							bmm.setMark(BlockMessageMarkEnum.DONE);
						} catch (Exception e1) {
							log.error("同步区块   ===>   请求重新同步上一个区块发生异常。", e1);
							bmm.setMark(BlockMessageMarkEnum.ERROR);
						}
						break;
					case CACHE:
						bsm.setWantHeight(block.getHeader().getHeight());
						bsm.setSyncCode(BlockSyncCodeEnum.LB);
						bmm.setMark(BlockMessageMarkEnum.DONE);
						break;
					case APPLY:
						BlockInfo parentBlock = fokBlockChain
								.getBlockByHash(block.getHeader().getParentHash().toByteArray());
						if (fastSync && block.getBody().getTxsCount() > 0) {
							for (TransactionInfo tx : block.getBody().getTxsList()) {
								fokTransaction.syncTransaction(tx, false, BigInteger.ZERO);
							}
						}
						BlockSyncMessage processMessage = processBlock(block, parentBlock);
						if (processMessage.getSyncTxHash() != null && processMessage.getSyncTxHash().size() > 0) {
							bsm.setSyncCode(BlockSyncCodeEnum.LT);
							bsm.setSyncTxHash(processMessage.getSyncTxHash());
							bsm.setWantHeight(block.getHeader().getHeight());

							bmm.setMark(BlockMessageMarkEnum.ERROR);
							break;
						}

						BlockInfo.Builder applyBlock = block.clone();

						boolean isValidateBlock = true;
						if (!BytesComparisons.equal(block.getHeader().getStateRoot().toByteArray(),
								applyBlock.getHeader().getStateRoot().toByteArray())) {
							log.warn("同步区块   ===>   账户状态不一致 [{}]!=[{}]",
									crypto.bytesToHexStr(block.getHeader().getStateRoot().toByteArray()),
									crypto.bytesToHexStr(applyBlock.getHeader().getStateRoot().toByteArray()));
							isValidateBlock = false;
						} else if (!BytesComparisons.equal(block.getHeader().getTransactionRoot().toByteArray(),
								applyBlock.getHeader().getTransactionRoot().toByteArray())) {
							log.warn("同步区块   ===>   交易内容不一致 [{}]!=[{}]",
									crypto.bytesToHexStr(block.getHeader().getTransactionRoot().toByteArray()),
									crypto.bytesToHexStr(applyBlock.getHeader().getTransactionRoot().toByteArray()));
							isValidateBlock = false;
						} else if (!BytesComparisons.equal(block.getHeader().getReceiptRoot().toByteArray(),
								applyBlock.getHeader().getReceiptRoot().toByteArray())) {
							log.warn("同步区块   ===>   交易执行结果不一致 [{}]!=[{}]",
									crypto.bytesToHexStr(block.getHeader().getReceiptRoot().toByteArray()),
									crypto.bytesToHexStr(applyBlock.getHeader().getReceiptRoot().toByteArray()));
							isValidateBlock = false;
						}

						if (!isValidateBlock) {
							final BlockHeader.Builder bbh = block.getHeader().toBuilder();

							threadExecutorManager.getOrCreateForkJoinPool(10).submit(new Runnable() {
								@Override
								public void run() {
									for (ByteString txHash : bbh.getTxHashsList()) {
										fokTransaction.getTmConfirmQueue().revalidate(txHash.toByteArray());
									}
								}
							});
							bmm.setMark(BlockMessageMarkEnum.ERROR);
						} else {
							bmm = fokBlockChain.tryConnectBlock(applyBlock.build());
							// TODO 测试如果不清理会产生什么结果
							// threadExecutorManager.getOrCreateForkJoinPool(10).submit(new
							// Runnable() {
							// @Override
							// public void run() {
							// fokTransaction.getTmConfirmQueue().clear();
							// }
							// });
						}
						break;
					case APPLY_CHILD:
						for (BlockMessage bm : bmm.getChildBlock()) {
							applyBlock = bm.getBlock().toBuilder();
							syncBlock(applyBlock, false);
						}
						bmm.setMark(BlockMessageMarkEnum.DONE);
						break;
					case STORE:
						bmm.setMark(BlockMessageMarkEnum.DONE);
						break;
					case ERROR:
						bmm.setMark(BlockMessageMarkEnum.DONE);
						break;
					default:
						break;
					}
				}
			}
		} catch (FokTrieRuntimeException e) {
			// TODO 尝试找到可用的区块
			// fokBlockChain.rollbackTo(applyBlock.getHeader().getNumber() -
			// 10);
		} catch (Exception e1) {
			log.error("", e1);
		}

		if (bsm.getCurrentHeight() == 0) {
			bsm.setCurrentHeight(fokBlockChain.getLastConnectedBlockHeight());
		}

		if (bsm.getWantHeight() == 0) {
			bsm.setWantHeight(bsm.getCurrentHeight());
		}

		log.warn(String.format("同步区块   ===>   高度[%-10s] hash[%s] 状态[%s] 丢失交易[%s] 丢失区块[%s] 当前区块[%s]",
				block.getHeader().getHeight(), crypto.bytesToHexStr(block.getHeader().getHash().toByteArray()),
				bsm.getSyncCode(), bsm.getSyncTxHash().size(), bsm.getWantHeight(), bsm.getCurrentHeight()));

		return bsm;
	}

	private BlockSyncMessage processBlock(BlockInfo.Builder currentBlock, BlockInfo parentBlock) throws Exception {
		return processBlock(currentBlock, parentBlock, null);
	}

	private BlockSyncMessage processBlock(BlockInfo.Builder currentBlock, BlockInfo parentBlock,
			List<TransactionInfo> createdtxs) throws Exception {
		BlockSyncMessage oBlockSyncMessage = new BlockSyncMessage();
		FokCacheTrie oTransactionTrie = new FokCacheTrie(crypto);
		FokCacheTrie oReceiptTrie = new FokCacheTrie(crypto);

		AccountInfo.Builder minner = fokAccount.getAccountOrCreate(currentBlock.getMiner().getAddress());
		if (currentBlock.getHeader().getHeight() >= 1 && !BytesComparisons
				.equal(parentBlock.getHeader().getStateRoot().toByteArray(), this.fokStateTrie.getRootHash())) {
			this.fokStateTrie.clear();
			this.fokStateTrie.setRoot(parentBlock.getHeader().getStateRoot().toByteArray());
		}

		byte[][] txTrieBB = new byte[currentBlock.getHeader().getTxHashsCount()][];
		TransactionInfo[] txs = new TransactionInfo[currentBlock.getHeader().getTxHashsCount()];
		int i = 0;
		BytesHashMap<AccountInfo.Builder> accounts = new BytesHashMap<>();
		accounts.put(minner.getAddress().toByteArray(), minner);

		if (createdtxs != null) {
			for (int dstIndex = 0; dstIndex < createdtxs.size(); dstIndex++) {
				TransactionInfo oTransactionInfo = createdtxs.get(dstIndex);
				txs[dstIndex] = oTransactionInfo;
				txTrieBB[dstIndex] = fokTransaction.getOriginalTransaction(oTransactionInfo);
			}
		} else {
			CountDownLatch cdl = new CountDownLatch(currentBlock.getHeader().getTxHashsCount());
			AtomicBoolean justCheck = new AtomicBoolean(false);
			ConcurrentLinkedQueue<byte[]> missingHash = new ConcurrentLinkedQueue<>();
			for (ByteString txHash : currentBlock.getHeader().getTxHashsList()) {
				threadExecutorManager.getOrCreateForkJoinPool(10)
						.submit(new ParalTransactionLoader(txHash.toByteArray(), i, cdl, txs, txTrieBB, accounts,
								missingHash, currentBlock.getHeader().getHeight(), justCheck));
				i++;
			}

			cdl.await();
			if (!missingHash.isEmpty()) {
				oBlockSyncMessage.setSyncCode(BlockSyncCodeEnum.LT);
				byte[] hash = missingHash.poll();
				while (hash != null) {
					oBlockSyncMessage.getSyncTxHash().add(hash);
					hash = missingHash.poll();
				}
				return oBlockSyncMessage;
			}
		}

		for (i = 0; i < currentBlock.getHeader().getTxHashsCount(); i++) {
			currentBlock.getBodyBuilder().addTxs(txs[i]);
			oTransactionTrie.put(FokRLP.encodeInt(i), txTrieBB[i]);
		}

		Map<String, ByteString> results = new ConcurrentHashMap<>();
		if (txs.length > 0) {
			oTransactionExecutorSeparator.reset();

			CountDownLatch cdl = new CountDownLatch(txs.length);

			for (i = 0; i < oTransactionExecutorSeparator.getBucketSize(); i++) {
				threadExecutorManager.getOrCreateForkJoinPool(10).submit(fokTransaction.new TransactionExecutor(
						oTransactionExecutorSeparator.getTxnQueue(i), currentBlock, accounts, results, cdl));
			}
			oTransactionExecutorSeparator.doClearing(txs);
			cdl.await();
		}

		fokAccount.addBalance(accounts.get(currentBlock.getMiner().getAddress().toByteArray()),
				BytesHelper.bytesToBigInteger(currentBlock.getMiner().getReward().toByteArray()));
		fokAccount.batchPutAccounts(accounts);

		Iterator<String> iter = results.keySet().iterator();
		List<String> keys = new ArrayList<>();
		while (iter.hasNext()) {
			String key = iter.next();
			keys.add(key);
		}
		Collections.sort(keys);
		for (String key : keys) {
			oReceiptTrie.put(FokRLP.encodeInt(keys.indexOf(key)), results.get(key).toByteArray());
		}
		currentBlock.getHeaderBuilder().setReceiptRoot(ByteString.copyFrom(
				oReceiptTrie.getRootHash() == null ? BytesHelper.EMPTY_BYTE_ARRAY : oReceiptTrie.getRootHash()));
		currentBlock.getHeaderBuilder().setTransactionRoot(ByteString.copyFrom(oTransactionTrie.getRootHash() == null
				? BytesHelper.EMPTY_BYTE_ARRAY : oTransactionTrie.getRootHash()));
		currentBlock.getHeaderBuilder().setStateRoot(ByteString.copyFrom(fokStateTrie.getRootHash()));

		oBlockSyncMessage.setSyncCode(BlockSyncCodeEnum.SS);
		return oBlockSyncMessage;
	}

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

		@Override
		public void run() {
			try {
				Thread.currentThread().setName("txloader-" + blocknumber);
				TransactionMessage tm = fokTransaction.removeWaitingSendOrBlockTx(txHash);
				TransactionInfo oTransactionInfo = null;
				if (tm != null) {
					oTransactionInfo = tm.getTx();
				}
				if (oTransactionInfo == null) {
					oTransactionInfo = fokTransaction.getTransaction(txHash);
				}
				if (oTransactionInfo == null || oTransactionInfo.getHash() == null
						|| oTransactionInfo.getBody().getInput() == null) {
					missingHash.add(txHash);
					justCheck.set(true);
				} else {
					if (!justCheck.get()) {
						bb[dstIndex] = oTransactionInfo;
						txTrieBB[dstIndex] = fokTransaction.getOriginalTransaction(oTransactionInfo);
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
}
