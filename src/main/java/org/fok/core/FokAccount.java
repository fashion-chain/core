package org.fok.core;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Provides;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import onight.osgi.annotation.NActorProvider;
import onight.tfw.ntrans.api.ActorService;
import onight.tfw.ntrans.api.annotation.ActorRequire;

import org.fok.core.config.CommonConstant;
import org.fok.core.cryptoapi.ICryptoHandler;
import org.fok.core.datasource.FokAccountDataAccess;
import org.fok.core.datasource.FokCommonDataAccess;
import org.fok.core.model.Account.*;
import org.fok.core.trie.FokStateTrie;
import org.fok.core.trie.FokStorageTrieCache;
import org.fok.tools.bytes.BytesHashMap;
import org.fok.tools.bytes.BytesHelper;

import com.google.protobuf.ByteString;

@NActorProvider
@Provides(specifications = { ActorService.class }, strategy = "SINGLETON")
@Instantiate(name = "fok_account_core")
@Slf4j
@Data
public class FokAccount implements ActorService {
	@ActorRequire(name = "fok_state_trie", scope = "global")
	FokStateTrie stateTrie;
	@ActorRequire(name = "Storage_TrieCache", scope = "global")
	FokStorageTrieCache storageTrieCache;
	@ActorRequire(name = "common_da", scope = "global")
	FokCommonDataAccess commonDA;
	@ActorRequire(name = "account_da", scope = "global")
	FokAccountDataAccess accountDA;
	@ActorRequire(name = "bc_crypto", scope = "global")
	ICryptoHandler crypto;

	public AccountInfo.Builder createAccount(ByteString address) {
		return createAccount(address, ByteString.copyFrom(BytesHelper.ZERO_BYTE_ARRAY),
				ByteString.copyFrom(BytesHelper.ZERO_BYTE_ARRAY), 0, null, null, null);
	}

	public AccountInfo.Builder createUnionAccount(ByteString address, ByteString max, ByteString acceptMax,
			int acceptLimit, List<ByteString> addresses) {
		return createAccount(address, max, acceptMax, acceptLimit, addresses, null, null);
	}

	private AccountInfo.Builder createAccount(ByteString address, ByteString max, ByteString acceptMax, int acceptLimit,
			List<ByteString> addresses, ByteString code, ByteString exdata) {
		AccountInfo.Builder oUnionAccount = AccountInfo.newBuilder();
		AccountValue.Builder oUnionAccountValue = AccountValue.newBuilder();

		oUnionAccountValue.setAcceptLimit(acceptLimit);
		oUnionAccountValue.setAcceptMax(acceptMax);
		if (addresses != null) {
			for (int i = 0; i < addresses.size(); i++) {
				oUnionAccountValue.addAddress(addresses.get(i));
			}
		}

		oUnionAccountValue.setBalance(ByteString.copyFrom(BytesHelper.ZERO_BYTE_ARRAY));
		oUnionAccountValue.setMax(max);
		oUnionAccountValue.setNonce(CommonConstant.Account_Default_Nonce.intValue());

		if (code != null) {
			oUnionAccountValue.setCode(code);
			oUnionAccountValue.setCodeHash(ByteString.copyFrom(crypto.sha3(code.toByteArray())));
		}

		if (exdata != null) {
			oUnionAccountValue.setData(exdata);
		}
		oUnionAccount.setAddress(address);
		oUnionAccount.setValue(oUnionAccountValue);
		return oUnionAccount;
	}

	public AccountInfo.Builder getAccount(ByteString addr) {
		try {
			if (this.stateTrie != null) {
				byte[] valueHash = this.stateTrie.get(addr.toByteArray());
				if (valueHash != null) {
					AccountInfo.Builder oAccountInfo = AccountInfo.newBuilder();
					oAccountInfo.getValueBuilder().mergeFrom(valueHash);
					oAccountInfo.setAddress(addr);
					return oAccountInfo;
				}
			}
		} catch (Exception e) {
			log.error("account not found::" + crypto.bytesToHexStr(addr.toByteArray()), e);
		}
		return null;
	}

	/**
	 * 根据地址获取账户。
	 * <p>
	 * 如果地址不存在，就根据地址创建一个新的账户对象
	 * </p>
	 * 
	 * @param addr
	 * @return
	 */
	public AccountInfo.Builder getAccountOrCreate(ByteString addr) {
		try {
			AccountInfo.Builder oAccount = getAccount(addr);
			if (oAccount == null) {
				oAccount = createAccount(addr);
			}
			return oAccount;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public List<AccountContractValue> getContractByCreator(ByteString addr) {
		try {
			return commonDA.getContractByCreator(addr.toByteArray());
		} catch (Throwable e) {
			log.error("cannot find contract::" + e);
		}
		return null;
	}

	public int IncreaseNonce(AccountInfo.Builder account) throws Exception {
		account.getValueBuilder().setNonce(account.getValue().getNonce() + 1);
		return account.getValue().getNonce();
	}

	public synchronized BigInteger addBalance(AccountInfo.Builder oAccount, BigInteger balance) throws Exception {
		AccountValue.Builder oAccountValue = oAccount.getValue().toBuilder();
		oAccountValue.setBalance(ByteString.copyFrom(BytesHelper.bigIntegerToBytes(
				balance.add(BytesHelper.bytesToBigInteger(oAccountValue.getBalance().toByteArray())))));
		oAccount.setValue(oAccountValue);
		return BytesHelper.bytesToBigInteger(oAccountValue.getBalance().toByteArray());
	}

	//
	// public synchronized BigInteger subBalance(AccountInfo.Builder oAccount,
	// BigInteger balance) throws Exception {
	// AccountValue.Builder oAccountValue = oAccount.getValue().toBuilder();
	// oAccountValue.setBalance(ByteString.copyFrom(BytesHelper.bigIntegerToBytes(
	// BytesHelper.bytesToBigInteger(oAccountValue.getBalance().toByteArray()).subtract(balance))));
	// oAccount.setValue(oAccountValue);
	// return
	// BytesHelper.bytesToBigInteger(oAccountValue.getBalance().toByteArray());
	// }
	//
	// public synchronized BigInteger addTokenBalance(AccountInfo.Builder oAccount,
	// byte[] token, BigInteger balance)
	// throws Exception {
	// AccountValue.Builder oAccountValue = oAccount.getValue().toBuilder();
	//
	// for (int i = 0; i < oAccountValue.getTokensCount(); i++) {
	// if ( oAccountValue.getTokens(i).getToken().toByteArray()) {
	// oAccountValue.setTokens(i,
	// oAccountValue.getTokens(i).toBuilder()
	// .setBalance(ByteString.copyFrom(BytesHelper.bigIntegerToBytes(balance.add(BytesHelper
	// .bytesToBigInteger(oAccountValue.getTokens(i).getBalance().toByteArray()))))));
	// oAccount.setValue(oAccountValue);
	// return
	// BytesHelper.bytesToBigInteger(oAccountValue.getTokens(i).getBalance().toByteArray());
	// }
	// }
	// // 如果token账户余额不存在，直接增加一条记录
	// AccountTokenValue.Builder oAccountTokenValue =
	// AccountTokenValue.newBuilder();
	// oAccountTokenValue.setBalance(ByteString.copyFrom(BytesHelper.bigIntegerToBytes(balance)));
	// oAccountTokenValue.setToken(ByteString.copyFrom(token));
	// oAccountValue.addTokens(oAccountTokenValue);
	// oAccount.setValue(oAccountValue);
	// return
	// BytesHelper.bytesToBigInteger(oAccountTokenValue.getBalance().toByteArray());
	// }
	//
	// public synchronized BigInteger subTokenBalance(AccountInfo.Builder oAccount,
	// String token, BigInteger balance)
	// throws Exception {
	// AccountValue.Builder oAccountValue = oAccount.getValue().toBuilder();
	//
	// for (int i = 0; i < oAccountValue.getTokensCount(); i++) {
	// if (oAccountValue.getTokens(i).getToken().equals(token)) {
	// oAccountValue.setTokens(i,
	// oAccountValue.getTokens(i).toBuilder()
	// .setBalance(ByteString.copyFrom(BytesHelper.bigIntegerToBytes(BytesHelper
	// .bytesToBigInteger(oAccountValue.getTokens(i).getBalance().toByteArray())
	// .subtract(balance)))));
	//
	// oAccount.setValue(oAccountValue);
	// return
	// BytesHelper.bytesToBigInteger(oAccountValue.getTokens(i).getBalance().toByteArray());
	// }
	// }
	// throw new FokAccountTokenNotFoundException("not found token" + token);
	// }
	//
	// public synchronized BigInteger addTokenLockBalance(AccountInfo.Builder
	// oAccount, String token, BigInteger balance)
	// throws Exception {
	// AccountValue.Builder oAccountValue = oAccount.getValue().toBuilder();
	//
	// for (int i = 0; i < oAccountValue.getTokensCount(); i++) {
	// if (oAccountValue.getTokens(i).getToken().equals(token)) {
	// oAccountValue.setTokens(i,
	// oAccountValue.getTokens(i).toBuilder()
	// .setLocked(ByteString.copyFrom(BytesHelper.bigIntegerToBytes(balance.add(BytesHelper
	// .bytesToBigInteger(oAccountValue.getTokens(i).getLocked().toByteArray()))))));
	// return
	// BytesHelper.bytesToBigInteger(oAccountValue.getTokens(i).getBalance().toByteArray());
	// }
	// }
	// AccountTokenValue.Builder oAccountTokenValue =
	// AccountTokenValue.newBuilder();
	// oAccountTokenValue.setLocked(ByteString.copyFrom(BytesHelper.bigIntegerToBytes(balance)));
	// oAccountTokenValue.setToken(token);
	// oAccountValue.addTokens(oAccountTokenValue);
	// oAccount.setValue(oAccountValue);
	// return
	// BytesHelper.bytesToBigInteger(oAccountTokenValue.getBalance().toByteArray());
	// }
	//
	// public BigInteger getTokenBalance(AccountInfo.Builder oAccount, String token)
	// throws Exception {
	// AccountValue.Builder oAccountValue = oAccount.getValue().toBuilder();
	// for (int i = 0; i < oAccountValue.getTokensCount(); i++) {
	// if (oAccountValue.getTokens(i).getToken().equals(token)) {
	// return
	// BytesHelper.bytesToBigInteger(oAccountValue.getTokens(i).getBalance().toByteArray());
	// }
	// }
	// return BigInteger.ZERO;
	// }
	//
	// public List<AccountCryptoToken> getCryptoTokenBalance(AccountInfo.Builder
	// oAccount, ByteString symbol)
	// throws Exception {
	// AccountValue.Builder oAccountValue = oAccount.getValue().toBuilder();
	//
	// for (int i = 0; i < oAccountValue.getCryptosCount(); i++) {
	// if (oAccountValue.getCryptos(i).getSymbolBytes().equals(symbol)) {
	// return oAccountValue.getCryptos(i).getTokensList();
	// }
	// }
	//
	// return new ArrayList<AccountCryptoToken>();
	// }
	//
	// public void createOrUpdateToken(ERC20TokenValue oICOValue) throws Exception {
	// commonDA.putToken(oICOValue);
	// }
	//
	// public List<ERC20TokenValue> getTokens(byte[] address, String token) {
	// try {
	// List<ERC20TokenValue> list = commonDA.getToken();
	// for (ERC20TokenValue erc20TokenValue : list) {
	// if (address != null && BytesComparisons.equal(address,
	// erc20TokenValue.getAddress().toByteArray())) {
	// list.add(erc20TokenValue);
	// } else if (StringUtils.isNotBlank(token) &&
	// token.equals(erc20TokenValue.getToken())) {
	// list.add(erc20TokenValue);
	// } else if (address == null && StringUtils.isBlank(token)) {
	// list.add(erc20TokenValue);
	// }
	// }
	// return list;
	// } catch (Exception e) {
	// log.error("error on query tokens::" + e);
	// }
	// return null;
	// }
	//
	// public boolean isExistsToken(String token) throws Exception {
	// ERC20TokenValue oERC20TokenValue = commonDA.getToken(token);
	// return oERC20TokenValue != null;
	// }
	//
	public void batchPutAccounts(BytesHashMap<AccountInfo.Builder> accountValues) {
		Set<byte[]> keySets = accountValues.keySet();
		Iterator<byte[]> iterator = keySets.iterator();
		List<String> keys = new ArrayList<>();
		while (iterator.hasNext()) {
			byte[] key = iterator.next();
			keys.add(crypto.bytesToHexStr(key));
		}
		Collections.sort(keys);
		for (String key : keys) {
			byte[] bytesKey = crypto.hexStrToBytes(key);
			AccountValue value = accountValues.get(bytesKey).getValue();
			this.stateTrie.put(bytesKey, value.toByteArray());
		}
		keys.clear();
	}
	//
	// public void putStorage(AccountInfo.Builder oAccount, byte[] key, byte[]
	// value) {
	// FokStorageTrie oStorage = getStorageTrie(oAccount);
	// oStorage.put(key, value);
	// byte[] rootHash = oStorage.getRootHash();
	//
	// AccountValue.Builder oAccountValue = oAccount.getValue().toBuilder();
	// oAccountValue.setStorage(ByteString.copyFrom(rootHash));
	// oAccount.setValue(oAccountValue);
	//
	// oStorage.clear();
	// oStorage = null;
	// }
	//
	// public void saveStorage(ByteString address, byte[] key, byte[] value) {
	// AccountInfo.Builder oAccount = getAccount(address);
	// putStorage(oAccount, key, value);
	// }
	//
	// public FokStorageTrie getStorageTrie(AccountInfo.Builder oAccount) {
	// FokStorageTrie oStorage =
	// storageTrieCache.get(encApi.hexEnc(oAccount.getAddress().toByteArray()));
	// if (oStorage == null) {
	// oStorage = new FokStorageTrie(this.accountDA, this.encApi);
	// if (oAccount == null || oAccount.getValue() == null ||
	// oAccount.getValue().getStorage() == null) {
	// oStorage.setRoot(BytesHelper.EMPTY_BYTE_ARRAY);
	// } else {
	// oStorage.setRoot(oAccount.getValue().getStorage().toByteArray());
	// }
	// storageTrieCache.put(encApi.hexEnc(oAccount.getAddress().toByteArray()),
	// oStorage);
	// }
	// return oStorage;
	// }
	//
	// public FokStorageTrie getStorageTrie(ByteString address) {
	// AccountInfo.Builder oAccount = getAccount(address);
	// return getStorageTrie(oAccount);
	// }
	//
	// public Map<String, byte[]> getStorage(AccountInfo.Builder oAccount,
	// List<byte[]> keys) {
	// Map<String, byte[]> storage = new HashMap<>();
	// FokStorageTrie oStorage = getStorageTrie(oAccount);
	// for (int i = 0; i < keys.size(); i++) {
	// storage.put(encApi.hexEnc(keys.get(i)), oStorage.get(keys.get(i)));
	// }
	// return storage;
	// }
	//
	// public byte[] getStorage(AccountInfo.Builder oAccount, byte[] key) {
	// FokStorageTrie oStorage = getStorageTrie(oAccount);
	// return oStorage.get(key);
	// }
	//
	// public boolean isExistsCryptoToken(String symbol, ByteString address, long
	// total, int codeCount)
	// throws InvalidProtocolBufferException, ODBException, InterruptedException,
	// ExecutionException {
	// CryptoTokenValue oCryptoTokenValue = commonDA.getCryptoToken(symbol);
	// if (oCryptoTokenValue == null) {
	// return true;
	// } else {
	// if (oCryptoTokenValue.getTotal() < oCryptoTokenValue.getCurrent() +
	// codeCount) {
	// return false;
	// } else if (!oCryptoTokenValue.getOwner().equals(address)) {
	// return false;
	// }
	// return true;
	// }
	// }
	//
	// public void createOrUpdateCryptoToken(CryptoTokenValue oCryptoTokenValue)
	// throws ODBException, InvalidProtocolBufferException, InterruptedException,
	// ExecutionException {
	// commonDA.putCryptoToken(oCryptoTokenValue);
	// }
}
