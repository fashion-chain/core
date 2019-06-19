package org.fok.core.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;

import org.apache.felix.ipojo.annotations.Instantiate;
import org.fok.core.datasource.FokBlockChainDataAccess;

import com.google.protobuf.Message;

import lombok.Data;
import lombok.Generated;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import onight.oapi.scala.commons.SessionModules;
import onight.osgi.annotation.NActorProvider;
import onight.tfw.mservice.NodeHelper;
import onight.tfw.outils.conf.PropHelper;

@NActorProvider
@Instantiate(name = "fok_chain_config")
@Slf4j
@Data
public class FokChainConfig extends SessionModules<Message> {
	@Override
	public String[] getCmds() {
		return new String[] { "CONFIG" };
	}

	@Override
	public String getModule() {
		return "CHAIN";
	}

	PropHelper prop = new PropHelper(null);

	boolean isNodeStart = false;
	String nodeId = "";
	byte[] nodeAddress;

	public final int account_storage_cache_size = prop.get(FokChainConfigKeys.account_storage_cache_nameId_key, 200);
	public final String account_storage_cache_nameId = prop.get(FokChainConfigKeys.account_storage_cache_nameId_key,
			"account");
	public final String transaction_message_queue_cache_nameId = prop
			.get(FokChainConfigKeys.transaction_message_queue_cache_nameId_key, "transaction_queue");
	public final String transaction_message_cache_nameId = prop
			.get(FokChainConfigKeys.transaction_message_cache_nameId_key, "transaction_cache");
	public final int transaction_message_queue_cache_size = prop
			.get(FokChainConfigKeys.transaction_message_queue_cache_size_key, 1000);
	public final String block_message_storage_cache_nameId = prop
			.get(FokChainConfigKeys.block_message_storage_cache_nameId_key, "transaction");
	public final int transaction_message_cache_size = prop.get(FokChainConfigKeys.transaction_message_cache_size_key,
			1000);

	public final int block_message_storage_cache_size = prop
			.get(FokChainConfigKeys.block_message_storage_cache_size_key, 1000);
	public final int transaction_confirm_queue_cache_size = prop
			.get(FokChainConfigKeys.transaction_confirm_queue_cache_size_key, 1000);
	public final String transaction_gas_address = prop.get(FokChainConfigKeys.transaction_gas_address_key, "");
	public final int block_stable_count = prop.get(FokChainConfigKeys.block_stable_count_key, 10);
	public final int block_version = prop.get(FokChainConfigKeys.block_version_key, 10);
	public final int block_reward = prop.get(FokChainConfigKeys.block_reward_key, 10);
	public final boolean environment_dev_mode = prop.get(FokChainConfigKeys.environment_mode_key, "").equals("dev");
	public final String environment_net = prop.get(FokChainConfigKeys.environment_net_key, "testnet");
	public final String environment_dev_num = prop.get(FokChainConfigKeys.environment_mode_dev_num_key,
			String.valueOf(Math.abs(NodeHelper.getCurrNodeListenOutPort() - 5100 + 1)));
	public final String environment_mode_dev_pwd = prop.get(FokChainConfigKeys.environment_mode_dev_pwd_key, "");
	// private BigInteger token_lock_balance = new
	// BigInteger(props().get("org.brewchain.token.lock.balance", "0"));
	// private BigInteger token_mint_balance = new
	// BigInteger(props().get("org.brewchain.token.mint.balance", "0"));
	// private BigInteger token_burn_balance = new
	// BigInteger(props().get("org.brewchain.token.burn.balance", "0"));
	// private BigInteger contract_lock_balance = new
	// BigInteger(props().get("org.brewchain.contract.lock.balance", "0"));
	// private BigInteger minerReward = new
	// BigInteger(props().get("block.miner.reward", "0"));
	// private BigInteger maxTokenTotal =
	// readBigIntegerValue("org.brewchain.token.max.total", "0");
	// private BigInteger minTokenTotal =
	// readBigIntegerValue("org.brewchain.token.min.total", "0");
	// private BigInteger minSanctionCost =
	// readBigIntegerValue("org.brewchain.sanction.cost", "0");
	// private BigInteger minVoteCost =
	// readBigIntegerValue("org.brewchain.vote.cost", "0");
	// private String pwd = props().get("org.bc.manage.node.dev.pwd",
	// KeyConstant.PWD);
	// private String keystoreNumber =
	// props().get("org.bc.manage.node.keystore.num",
	// String.valueOf(Math.abs(NodeHelper.getCurrNodeListenOutPort() - 5100 + 1)));
	// private int stableBlocks = props().get("org.brewchain.stable.blocks",
	// KeyConstant.STABLE_BLOCK);
	// private String lock_account_address =
	// props().get("org.brewchain.account.lock.address", null);
	// private boolean isDev = props().get("org.brewchain.man.dev",
	// "true").equals("true");
	// private int defaultRollBackCount =
	// props().get("org.brewchain.manage.rollback.count", 1);
	// private int accountVersion = props().get("org.brewchain.account.version", 0);
	// private int blockEpochMSecond = props().get("org.bc.dpos.blk.epoch.ms", 1000)
	// / 1000;
	// private int blockEpochSecond = props().get("org.bc.dpos.blk.epoch.sec", 1);
	// private String nodeAccount = props().get("org.bc.manage.node.account",
	// KeyConstant.DB_NODE_ACCOUNT_STR);
	// private String adminKey = props().get("org.bc.manage.admin.account",
	// KeyConstant.DB_ADMINISTRATOR_KEY_STR);
	// private String nodeNet = props().get("org.bc.manage.node.net",
	// KeyConstant.DB_NODE_NET_STR);
	// private String net = readNet();
	// private int cacheTxInitSize =
	// props().get("org.brewchain.account.cache.tx.init", 10000);
	// private long cacheTxMaximumSize =
	// props().get("org.brewchain.account.cache.tx.max", 1000000);

	private BigInteger readBigIntegerValue(String key, String defaultVal) {
		try {
			return new BigInteger(prop.get(key, defaultVal));
		} catch (Exception e) {
			log.error("cannot read key::" + key, e);
		}
		return new BigInteger(defaultVal);
	}

	private String readNet() {
		String network = "";
		BufferedReader br = null;
		FileReader fr = null;
		try {
			File networkFile = new File(".chainnet");
			if (!networkFile.exists() || !networkFile.canRead()) {
				// read default config
				network = environment_net;
			}
			if (network == null || network.isEmpty()) {
				while (!networkFile.exists() || !networkFile.canRead()) {
					log.debug("waiting chain_net config...");
					Thread.sleep(1000);
				}

				fr = new FileReader(networkFile.getPath());
				br = new BufferedReader(fr);
				String line = br.readLine();
				if (line != null) {
					network = line.trim().replace("\r", "").replace("\t", "");
				}

			}
		} catch (Exception e) {
			log.error("fail to read net config");
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
				}
			}
			if (fr != null) {
				try {
					fr.close();
				} catch (IOException e) {
				}
			}
		}
		return network;
	}
}
