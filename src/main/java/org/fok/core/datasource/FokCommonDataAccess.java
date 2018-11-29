package org.fok.core.datasource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Provides;
import org.fok.core.bean.BlockMessageBuffer;
import org.fok.core.config.CommonConstant;
import org.fok.core.config.FokChainConfig;
import org.fok.core.dbapi.ODBException;
import org.fok.core.model.Account.AccountContract;
import org.fok.core.model.Account.AccountContractValue;
import org.fok.tools.bytes.BytesComparisons;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.sf.ehcache.Cache;
import onight.osgi.annotation.NActorProvider;
import onight.tfw.ntrans.api.ActorService;

@NActorProvider
@Provides(specifications = { ActorService.class }, strategy = "SINGLETON")
@Instantiate(name = "common_da")
@Slf4j
@Data
public class FokCommonDataAccess extends BaseDatabaseAccess {
	// @StoreDAO(target = daoProviderId, daoClass = FokAccountDao.class)
	// ODBSupport<byte[], byte[]> commonDao;

	public byte[] getNodeAccountAddress() throws ODBException, InterruptedException, ExecutionException {
		return get(dao, CommonConstant.Node_Account_Address);
	}

	public void setNodeAccountAddress(byte[] address) throws ODBException, InterruptedException, ExecutionException {
		put(dao, CommonConstant.Node_Account_Address, address);
	}

	/**
	 * Return contract info created by creator
	 * 
	 * @param address
	 *            - creator
	 * @return
	 * @throws InvalidProtocolBufferException
	 * @throws ODBException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public List<AccountContractValue> getContractByCreator(byte[] address) throws Exception {
		List<AccountContractValue> contracts = new ArrayList<>();
		AccountContract oAccountContract = null;
		byte[] v = get(dao, CommonConstant.Exists_Contract);
		if (v != null) {
			oAccountContract = AccountContract.parseFrom(v);
		}
		if (oAccountContract != null) {
			for (AccountContractValue oAccountContractValue : oAccountContract.getValueList()) {
				if (BytesComparisons.equal(address, oAccountContractValue.getAddress().toByteArray())) {
					contracts.add(oAccountContractValue);
				}
			}
		}
		return contracts;
	}

	// public List<TokenValue> getToken()
	// throws ODBException, InterruptedException, ExecutionException,
	// InvalidProtocolBufferException {
	// byte[] v = get(dao, CommonConstant.Exists_Token);
	// List<TokenValue> ret = new ArrayList<>();
	// if (v != null) {
	// Token oToken = Token.parseFrom(v);
	// for (TokenValue oExistsTokenValue : oToken.getValueList()) {
	// ret.add(oExistsTokenValue);
	// }
	// }
	// return ret;
	// }
	//
	// public ERC20TokenValue getToken(String token)
	// throws ODBException, InterruptedException, ExecutionException,
	// InvalidProtocolBufferException {
	// byte[] v = get(dao, CommonConstant.Exists_Token);
	// if (v != null) {
	// ERC20Token oERC20Token = ERC20Token.parseFrom(v);
	// for (ERC20TokenValue oExistsERC20TokenValue : oERC20Token.getValueList()) {
	// if (oExistsERC20TokenValue.getToken().equals(token)) {
	// return oExistsERC20TokenValue;
	// }
	// }
	// }
	// return null;
	// }
	//
	// /**
	// * 创建或更新token
	// * <p>
	// * 如果token已经存在，则更新为新值。如果token不存在，则创建一个。
	// *
	// * @param oICOValue
	// * @throws ODBException
	// * @throws InterruptedException
	// * @throws ExecutionException
	// * @throws InvalidProtocolBufferException
	// */
	// public void putToken(ERC20TokenValue oICOValue)
	// throws ODBException, InterruptedException, ExecutionException,
	// InvalidProtocolBufferException {
	// byte[] v = get(dao, CommonConstant.Exists_Token);
	// ERC20Token.Builder oERC20Token;
	// if (v == null) {
	// oERC20Token = ERC20Token.newBuilder();
	// oERC20Token.addValue(oICOValue);
	// } else {
	// oERC20Token = ERC20Token.parseFrom(v).toBuilder();
	//
	// boolean isExists = false;
	// for (ERC20TokenValue.Builder oExistsERC20TokenValue :
	// oERC20Token.getValueBuilderList()) {
	// if (oExistsERC20TokenValue.getToken().equals(oICOValue.getToken())) {
	// isExists = true;
	// oExistsERC20TokenValue = oICOValue.toBuilder();
	// }
	// }
	// if (!isExists) {
	// oERC20Token.addValue(oICOValue);
	// }
	// }
	// put(dao, CommonConstant.Exists_Token, oERC20Token.build().toByteArray());
	// }
	//
	// public List<CryptoTokenValue> getCryptoToken()
	// throws ODBException, InterruptedException, ExecutionException,
	// InvalidProtocolBufferException {
	// byte[] v = get(dao, CommonConstant.Exists_Crypto_Token);
	// List<CryptoTokenValue> ret = new ArrayList<>();
	// if (v != null) {
	// CryptoToken oCryptoToken = CryptoToken.parseFrom(v);
	// for (CryptoTokenValue oCryptoTokenValue : oCryptoToken.getValueList()) {
	// ret.add(oCryptoTokenValue);
	// }
	// }
	// return ret;
	// }
	//
	// public CryptoTokenValue getCryptoToken(String symbol)
	// throws InvalidProtocolBufferException, ODBException, InterruptedException,
	// ExecutionException {
	// byte[] v = get(dao, CommonConstant.Exists_Token);
	// if (v != null) {
	// CryptoToken oCryptoToken = CryptoToken.parseFrom(v);
	// for (CryptoTokenValue oCryptoTokenValue : oCryptoToken.getValueList()) {
	// if (oCryptoTokenValue.getSymbol().equals(symbol)) {
	// return oCryptoTokenValue;
	// }
	// }
	// }
	// return null;
	// }
	//
	// public void putCryptoToken(CryptoTokenValue oNewCryptoTokenValue)
	// throws ODBException, InterruptedException, ExecutionException,
	// InvalidProtocolBufferException {
	// byte[] v = get(dao, CommonConstant.Exists_Crypto_Token);
	// CryptoToken.Builder oCryptoToken;
	// if (v == null) {
	// oCryptoToken = CryptoToken.newBuilder();
	// oCryptoToken.addValue(oNewCryptoTokenValue);
	// } else {
	// oCryptoToken = CryptoToken.parseFrom(v).toBuilder();
	//
	// boolean isExists = false;
	// for (CryptoTokenValue.Builder oCryptoTokenValue :
	// oCryptoToken.getValueBuilderList()) {
	// if (oCryptoTokenValue.getSymbol().equals(oNewCryptoTokenValue.getSymbol())) {
	// isExists = true;
	// oCryptoTokenValue = oNewCryptoTokenValue.toBuilder();
	// }
	// }
	// if (!isExists) {
	// oCryptoToken.addValue(oNewCryptoTokenValue);
	// }
	// }
	// put(dao, CommonConstant.Exists_Crypto_Token,
	// oCryptoToken.build().toByteArray());
	// }
}
