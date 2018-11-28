package org.fok.core.handler;

import java.math.BigInteger;
import java.util.List;

import org.apache.felix.ipojo.util.Log;
import org.fok.core.FokTransaction;
import org.fok.core.api.IAccountHandler;
import org.fok.core.cryptoapi.ICryptoHandler;
import org.fok.core.datasource.FokAccountDataAccess;
import org.fok.core.model.Account.AccountCryptoToken;
import org.fok.core.model.Account.AccountCryptoValue;
import org.fok.core.model.Account.AccountInfo;
import org.fok.core.model.Account.AccountTokenValue;
import org.fok.core.model.Account.AccountInfo.Builder;
import org.fok.core.model.Account.AccountValue;
import org.fok.core.model.Account.CryptoTokenValue;
import org.fok.core.model.Account.TokenValue;
import org.fok.core.trie.FokStorageTrie;
import org.fok.core.trie.FokStorageTrieCache;
import org.fok.tools.bytes.BytesComparisons;
import org.fok.tools.bytes.BytesHelper;

import com.google.protobuf.ByteString;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AccountHandler implements IAccountHandler {
	private FokStorageTrieCache storageTrieCache;
	private FokAccountDataAccess accountDA;
	private ICryptoHandler cryptoHandler;

	@Override
	public BigInteger addBalance(Builder account, BigInteger amount) {
		account.getValueBuilder().setBalance(ByteString.copyFrom(BytesHelper.bigIntegerToBytes(
				BytesHelper.bytesToBigInteger(account.getValue().getBalance().toByteArray()).add(amount))));
		return null;
	}

	@Override
	public void addCryptoTokenBalance(Builder account, byte[] symbol, AccountCryptoToken oAccountCryptoToken) {
		AccountValue.Builder oAccountValue = account.getValueBuilder();
		for (AccountCryptoValue.Builder acv : oAccountValue.getCryptosBuilderList()) {
			if (BytesComparisons.equal(acv.getSymbol().toByteArray(), symbol)) {
				boolean isNew = true;
				for (AccountCryptoToken.Builder act : acv.getTokensBuilderList()) {
					if (BytesComparisons.equal(act.getHash().toByteArray(),
							oAccountCryptoToken.getHash().toByteArray())) {
						act = oAccountCryptoToken.toBuilder();
						isNew = false;
						break;
					}
				}

				if (isNew) {
					acv.addTokens(oAccountCryptoToken);
				}
				break;
			}
		}
	}

	@Override
	public BigInteger addTokenBalance(Builder account, byte[] token, BigInteger amount) {
		AccountValue.Builder oAccountValue = account.getValueBuilder();
		for (AccountTokenValue.Builder atv : oAccountValue.getTokensBuilderList()) {
			if (BytesComparisons.equal(atv.getToken().toByteArray(), token)) {
				atv.setBalance(ByteString.copyFrom(BytesHelper
						.bigIntegerToBytes(BytesHelper.bytesToBigInteger(atv.getBalance().toByteArray()).add(amount))));
				return BytesHelper.bytesToBigInteger(atv.getBalance().toByteArray());
			}
		}
		AccountTokenValue.Builder oAccountTokenValue = AccountTokenValue.newBuilder();
		oAccountTokenValue.setToken(ByteString.copyFrom(token));
		oAccountTokenValue.setBalance(ByteString.copyFrom(BytesHelper.bigIntegerToBytes(amount)));
		oAccountValue.addTokens(oAccountTokenValue);
		return amount;
	}

	@Override
	public BigInteger addTokenFreezeBalance(Builder account, byte[] token, BigInteger amount) {
		AccountValue.Builder oAccountValue = account.getValueBuilder();
		for (AccountTokenValue.Builder atv : oAccountValue.getTokensBuilderList()) {
			if (BytesComparisons.equal(atv.getToken().toByteArray(), token)) {
				atv.setFreeze(ByteString.copyFrom(BytesHelper
						.bigIntegerToBytes(BytesHelper.bytesToBigInteger(atv.getFreeze().toByteArray()).add(amount))));
				return BytesHelper.bytesToBigInteger(atv.getFreeze().toByteArray());
			}
		}
		AccountTokenValue.Builder oAccountTokenValue = AccountTokenValue.newBuilder();
		oAccountTokenValue.setToken(ByteString.copyFrom(token));
		oAccountTokenValue.setFreeze(ByteString.copyFrom(BytesHelper.bigIntegerToBytes(amount)));
		oAccountValue.addTokens(oAccountTokenValue);
		return amount;
	}

	@Override
	public BigInteger addTokenLockedBalance(Builder account, byte[] token, BigInteger amount) {
		AccountValue.Builder oAccountValue = account.getValueBuilder();
		for (AccountTokenValue.Builder atv : oAccountValue.getTokensBuilderList()) {
			if (BytesComparisons.equal(atv.getToken().toByteArray(), token)) {
				atv.setLocked(ByteString.copyFrom(BytesHelper
						.bigIntegerToBytes(BytesHelper.bytesToBigInteger(atv.getLocked().toByteArray()).add(amount))));
				return BytesHelper.bytesToBigInteger(atv.getLocked().toByteArray());
			}
		}
		AccountTokenValue.Builder oAccountTokenValue = AccountTokenValue.newBuilder();
		oAccountTokenValue.setToken(ByteString.copyFrom(token));
		oAccountTokenValue.setLocked(ByteString.copyFrom(BytesHelper.bigIntegerToBytes(amount)));
		oAccountValue.addTokens(oAccountTokenValue);
		return amount;
	}

	@Override
	public Builder createAccount(byte[] address) {
		return createUnionAccount(ByteString.copyFrom(address), ByteString.copyFrom(BytesHelper.ZERO_BYTE_ARRAY),
				ByteString.copyFrom(BytesHelper.ZERO_BYTE_ARRAY), 0, null);
	}

	@Override
	public Builder createContract(byte[] address, byte[] code, byte[] codeHash, byte[] data) {
		AccountInfo.Builder oContract = AccountInfo.newBuilder();
		AccountValue.Builder oContractValue = oContract.getValueBuilder();

		oContractValue.setBalance(ByteString.copyFrom(BytesHelper.ZERO_BYTE_ARRAY));
		oContractValue.setNonce(0);
		oContractValue.setCode(ByteString.copyFrom(code));
		oContractValue.setCodeHash(ByteString.copyFrom(codeHash));
		oContractValue.setData(ByteString.copyFrom(data));
		return oContract;
	}

	@Override
	public Builder createUnionAccount(ByteString address, ByteString max, ByteString acceptMax, int acceptLimit,
			List<ByteString> subAddress) {
		AccountInfo.Builder oUnionAccount = AccountInfo.newBuilder();
		AccountValue.Builder oUnionAccountValue = AccountValue.newBuilder();

		oUnionAccountValue.setAcceptLimit(acceptLimit);
		oUnionAccountValue.setAcceptMax(acceptMax);
		if (subAddress != null) {
			for (int i = 0; i < subAddress.size(); i++) {
				oUnionAccountValue.addAddress(subAddress.get(i));
			}
		}
		oUnionAccountValue.setBalance(ByteString.copyFrom(BytesHelper.ZERO_BYTE_ARRAY));
		oUnionAccountValue.setMax(max);
		oUnionAccountValue.setNonce(0);
		oUnionAccount.setAddress(address);
		oUnionAccount.setValue(oUnionAccountValue);
		return oUnionAccount;
	}

	@Override
	public byte[] cryptoTokenValueAddress() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] getAccountStorage(Builder account, byte[] key) {
		FokStorageTrie oFokStorageTrie = getAccountStorageTrie(account);
		return oFokStorageTrie.get(key);
	}

	@Override
	public BigInteger getBalance(Builder account) {
		return BytesHelper.bytesToBigInteger(account.getValue().getBalance().toByteArray());
	}

	@Override
	public CryptoTokenValue getCryptoToken(Builder account, byte[] symbol) {
		byte[] ctvBytes = getAccountStorage(account, symbol);
		if (ctvBytes == null) {
			return null;
		}
		try {
			CryptoTokenValue oCryptoTokenValue = CryptoTokenValue.parseFrom(ctvBytes);
			return oCryptoTokenValue;
		} catch (Exception e) {
			log.error(String.format("无法从账户[%s]中获取symbol名称为[%s]的crypto-token的发行信息",
					cryptoHandler.bytesToHexStr(account.getAddress().toByteArray()),
					cryptoHandler.bytesToHexStr(symbol)));
		}
		return null;
	}

	@Override
	public AccountCryptoToken getCryptoTokenBalance(Builder account, byte[] symbol, byte[] hash) {
		for (AccountCryptoValue acv : account.getValue().getCryptosList()) {
			if (BytesComparisons.equal(acv.getSymbol().toByteArray(), symbol)) {
				for (AccountCryptoToken act : acv.getTokensList()) {
					if (BytesComparisons.equal(act.getHash().toByteArray(), hash)) {
						return act;
					}
				}
			}
		}
		return null;
	}

	@Override
	public int getNonce(Builder account) {
		return account.getValue().getNonce();
	}

	@Override
	public TokenValue getToken(Builder account, byte[] token) {
		byte[] tvBytes = getAccountStorage(account, token);
		if (tvBytes != null) {
			try {
				TokenValue oTokenValue = TokenValue.parseFrom(tvBytes);
				return oTokenValue;
			} catch (Exception e) {
				log.error(String.format("无法从账户[%s]中获取token名称为[%s]的token的发行信息",
						cryptoHandler.bytesToHexStr(account.getAddress().toByteArray()),
						cryptoHandler.bytesToHexStr(token)));
			}
		}
		return null;
	}

	@Override
	public BigInteger getTokenBalance(Builder account, byte[] token) {
		for (AccountTokenValue atv : account.getValue().getTokensList()) {
			if (BytesComparisons.equal(atv.getToken().toByteArray(), token)) {
				return BytesHelper.bytesToBigInteger(atv.getBalance().toByteArray());
			}
		}
		return BigInteger.ONE.negate();
	}

	@Override
	public BigInteger getTokenFreezeBalance(Builder account, byte[] token) {
		for (AccountTokenValue atv : account.getValue().getTokensList()) {
			if (BytesComparisons.equal(atv.getToken().toByteArray(), token)) {
				if (atv.getFreeze() == null || atv.getFreeze().equals(ByteString.EMPTY)) {
					return BigInteger.ZERO;
				}
				return BytesHelper.bytesToBigInteger(atv.getFreeze().toByteArray());
			}
		}
		return BigInteger.ONE.negate();
	}

	@Override
	public BigInteger getTokenLockedBalance(Builder account, byte[] token) {
		for (AccountTokenValue atv : account.getValue().getTokensList()) {
			if (BytesComparisons.equal(atv.getToken().toByteArray(), token)) {
				if (atv.getLocked() == null || atv.getLocked().equals(ByteString.EMPTY)) {
					return BigInteger.ZERO;
				}
				return BytesHelper.bytesToBigInteger(atv.getLocked().toByteArray());
			}
		}
		return BigInteger.ONE.negate();
	}

	@Override
	public int increaseNonce(Builder account) {
		account.getValueBuilder().setNonce(account.getValue().getNonce() + 1);
		return account.getValue().getNonce();
	}

	@Override
	public byte[] lockBalanceAddress() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void putAccountStorage(Builder account, byte[] key, byte[] value) {
		FokStorageTrie oFokStorageTrie = getAccountStorageTrie(account);
		oFokStorageTrie.put(key, value);
		account.getValueBuilder().setStorage(ByteString.copyFrom(oFokStorageTrie.getRootHash()));
	}

	@Override
	public void putCryptoToken(Builder account, byte[] symbol, CryptoTokenValue oCryptoTokenValue) {
		putAccountStorage(account, symbol, oCryptoTokenValue.toByteArray());
	}

	@Override
	public void putToken(Builder account, byte[] token, TokenValue oTokenValue) {
		putAccountStorage(account, token, oTokenValue.toByteArray());
	}

	@Override
	public void removeCryptoTokenBalance(Builder account, byte[] symbol, byte[] hash) {
		AccountValue.Builder oAccountValue = account.getValueBuilder();
		for (AccountCryptoValue.Builder acv : oAccountValue.getCryptosBuilderList()) {
			if (BytesComparisons.equal(acv.getSymbol().toByteArray(), symbol)) {
				for (int j = 0; j < acv.getTokensCount(); j++) {
					if (BytesComparisons.equal(acv.getTokensBuilderList().get(j).getHash().toByteArray(), hash)) {
						acv.removeTokens(j);
						break;
					}
				}
			}
		}
	}

	@Override
	public BigInteger subBalance(Builder account, BigInteger amount) {
		account.getValueBuilder().setBalance(ByteString.copyFrom(BytesHelper.bigIntegerToBytes(
				BytesHelper.bytesToBigInteger(account.getValue().getBalance().toByteArray()).subtract(amount))));
		return BytesHelper.bytesToBigInteger(account.getValue().getBalance().toByteArray());
	}

	@Override
	public BigInteger subTokenBalance(Builder account, byte[] token, BigInteger amount) {
		AccountValue.Builder oAccountValue = account.getValueBuilder();
		for (AccountTokenValue.Builder atv : oAccountValue.getTokensBuilderList()) {
			if (BytesComparisons.equal(atv.getToken().toByteArray(), token)) {
				atv.setBalance(ByteString.copyFrom(BytesHelper.bigIntegerToBytes(
						BytesHelper.bytesToBigInteger(atv.getBalance().toByteArray()).subtract(amount))));
				return BytesHelper.bytesToBigInteger(atv.getBalance().toByteArray());
			}
		}
		return BigInteger.ONE.negate();
	}

	@Override
	public BigInteger subTokenFreezeBalance(Builder account, byte[] token, BigInteger amount) {
		AccountValue.Builder oAccountValue = account.getValueBuilder();
		for (AccountTokenValue.Builder atv : oAccountValue.getTokensBuilderList()) {
			if (BytesComparisons.equal(atv.getToken().toByteArray(), token)) {
				atv.setFreeze(ByteString.copyFrom(BytesHelper.bigIntegerToBytes(
						BytesHelper.bytesToBigInteger(atv.getFreeze().toByteArray()).subtract(amount))));
				return BytesHelper.bytesToBigInteger(atv.getFreeze().toByteArray());
			}
		}
		return BigInteger.ONE.negate();
	}

	@Override
	public BigInteger subTokenLockedBalance(Builder account, byte[] token, BigInteger amount) {
		AccountValue.Builder oAccountValue = account.getValueBuilder();
		for (AccountTokenValue.Builder atv : oAccountValue.getTokensBuilderList()) {
			if (BytesComparisons.equal(atv.getToken().toByteArray(), token)) {
				atv.setLocked(ByteString.copyFrom(BytesHelper.bigIntegerToBytes(
						BytesHelper.bytesToBigInteger(atv.getLocked().toByteArray()).subtract(amount))));
				return BytesHelper.bytesToBigInteger(atv.getLocked().toByteArray());
			}
		}
		return BigInteger.ONE.negate();
	}

	@Override
	public byte[] tokenValueAddress() {
		// TODO Auto-generated method stub
		return null;
	}

	private FokStorageTrie getAccountStorageTrie(Builder account) {
		FokStorageTrie oStorage = storageTrieCache.get(account.getAddress().toByteArray());
		if (oStorage == null) {
			oStorage = new FokStorageTrie(accountDA, cryptoHandler);
			if (account == null || account.getValue() == null || account.getValue().getStorage() == null) {
				oStorage.setRoot(BytesHelper.EMPTY_BYTE_ARRAY);
			} else {
				oStorage.setRoot(account.getValue().getStorage().toByteArray());
			}
			storageTrieCache.put(account.getAddress().toByteArray(), oStorage);
		}
		return oStorage;
	}
}
