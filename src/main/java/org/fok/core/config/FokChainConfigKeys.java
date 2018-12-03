package org.fok.core.config;

import lombok.Data;

@Data
public class FokChainConfigKeys {
	public static final String account_state_trie_cache_nameId_key = "org.fok.core.account.state.trie.cache.name";
	public static final String account_state_trie_cache_size_key = "org.fok.core.account.state.trie.cache.size";
	public static final String account_storage_cache_nameId_key = "org.fok.core.account.storage.cache.name";
	public static final String account_storage_cache_size_key = "org.fok.core.account.storage.cache.size";

	public static final String transaction_confirm_queue_cache_size_key = "org.fok.core.transaction.confirm.cache.size";
	public static final String transaction_gas_address_key = "org.fok.core.transaction.gas.address";
	public static final String transaction_message_queue_cache_nameId_key = "org.fok.core.transaction.message.queue.cache.name";
	public static final String transaction_message_queue_cache_size_key = "org.fok.core.transaction.message.queue.cache.size";
	public static final String transaction_message_cache_nameId_key = "org.fok.core.transaction.message.cache.name";
	public static final String transaction_message_cache_size_key = "org.fok.core.transaction.message.cache.size";

	public static final String block_stable_count_key = "org.fok.core.block.stable.count";
	public static final String block_version_key = "org.fok.core.block.version";
	public static final String block_reward_key = "org.fok.core.block.reward";
	public static final String block_message_storage_cache_nameId_key = "org.fok.core.block.message.cache.name";
	public static final String block_message_storage_cache_size_key = "org.fok.core.block.message.cache.size";

	public static final String environment_mode_key = "org.fok.core.environment.mode";
	public static final String environment_net_key = "org.fok.core.environment.net";
	public static final String environment_mode_dev_num_key = "org.fok.core.environment.mode.dev.num";
	public static final String environment_mode_dev_pwd_key = "org.fok.core.environment.mode.dev.password";
}
