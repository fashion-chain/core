package org.fok.core.bean;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Provides;
import org.fok.core.config.FokChainConfig;
import org.fok.core.config.FokChainConfigKeys;
import org.fok.core.model.Transaction.TransactionInfo;
import org.fok.tools.bytes.BytesHashMap;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import onight.osgi.annotation.NActorProvider;
import onight.tfw.ntrans.api.ActorService;
import onight.tfw.ntrans.api.annotation.ActorRequire;
import onight.tfw.outils.conf.PropHelper;

@NActorProvider
@Provides(specifications = { ActorService.class }, strategy = "SINGLETON")
@Instantiate(name = "fok_transaction_confirm_queue")
@Slf4j
@Data
public class TransactionConfirmQueue implements ActorService {
	PropHelper prop = new PropHelper(null);
	protected BytesHashMap<TransactionMessage> storage = new BytesHashMap<>();
	protected BytesHashMap<Long> rmStorage = new BytesHashMap<>();
	protected LinkedBlockingDeque<TransactionMessage> tmConfirmQueue = new LinkedBlockingDeque<>();
	AtomicBoolean gcRunningChecker = new AtomicBoolean(false);
	AtomicBoolean gcShouldStopChecker = new AtomicBoolean(false);
	int maxElementsInMemory = 0;
	long lastClearStorage = 0;
	long lastClearTime = 0;
	long clearWaitMS = 10000;

	public TransactionConfirmQueue() {
		maxElementsInMemory = prop.get(FokChainConfigKeys.transaction_confirm_queue_cache_size_key, 1000);
		clearWaitMS = prop.get(FokChainConfigKeys.transaction_confirm_queue_cache_clear_ms, 10000);
	}
	
	public int size() {
		return tmConfirmQueue.size();
	}

	public boolean containsKey(byte[] txHash) {
		if (this.storage == null) {
			return false;
		}
		return this.storage.containsKey(txHash);
	}
	
	public boolean containsConfirm(String txhash, int bit) {
		TransactionMessage tm = storage.get(txhash);
		if (tm != null) {
			return tm.getBits().testBit(bit);
		}
		return false;
	}

	public void put(TransactionMessage tm, BigInteger bits) {
		try {
			boolean put2Queue = false;
			TransactionMessage _tm = this.storage.get(tm.getKey());
			if (_tm == null) {
				synchronized (("acct_" + tm.getKey()[0]).intern()) {
					_tm = this.storage.get(tm.getKey());
					if (_tm == null) {
						put(tm.getKey(), tm);
						if (tm.getTx() != null) {
							put2Queue = true;
							tmConfirmQueue.addLast(tm);
						}
						_tm = tm;
					}
				}
			}
			if (!put2Queue && _tm.getTx() == null && tm.getTx() != null) {
				_tm.setTx(tm.getTx());
				_tm.setNeedBroadcast(tm.isNeedBroadcast());
				tmConfirmQueue.addLast(_tm);
			}
			_tm.setBits(bits);
		} catch (Exception e) {
			log.error("", e);
		} finally {
		}
	}

	public void increaseConfirm(byte[] txHash, BigInteger bits) {
		try {
			if (rmStorage.containsKey(txHash)) {
				return;
			}
			TransactionMessage tm = storage.get(txHash);
			if (tm == null) {
				synchronized (("acct_" + txHash[0]).intern()) {
					tm = storage.get(txHash);
					if (tm == null) {
						tm = new TransactionMessage(txHash, null, false);
						put(txHash, tm);
					}
				}
			}
			tm.setBits(bits);
		} catch (Exception e) {
			log.error("" + e);
		} finally {
		}
	}

	public List<TransactionInfo> poll(int maxsize) {
		return poll(maxsize, 0);
	}

	public List<TransactionInfo> poll(int maxsize, int minConfirm) {
		int i = 0;
		int maxtried = tmConfirmQueue.size();
		List<TransactionInfo> ret = new ArrayList<>();
		long checkTime = System.currentTimeMillis();
		gcShouldStopChecker.set(true);

		while (i < maxtried && ret.size() < maxsize) {
			TransactionMessage tm = tmConfirmQueue.pollFirst();
			if (tm == null) {
				break;
			} else {
				// rwLock.writeLock().lock();
				try {
					if (!tm.isRemoved() && !rmStorage.containsKey(tm.getKey()) && tm.getTx() != null) {
						if (tm.getBits().bitCount() >= minConfirm) {
							ret.add(tm.getTx());
							rmStorage.put(tm.getKey(), System.currentTimeMillis());
							tm.setRemoved(true);
							i++;
						} else {
							if (checkTime - tm.getLastUpdateTime() >= 180000) {
								if (tm.getTx() != null && tm.getTx() != null) {
									tmConfirmQueue.addLast(tm);
								} else {
									tm.setRemoved(true);
									rmStorage.put(tm.getKey(), System.currentTimeMillis());
								}
							} else {
								tmConfirmQueue.addLast(tm);
							}
							i++;
						}
					}
				} catch (Exception e) {
					log.error("", e);
				} finally {

				}
			}
		}
		return ret;
	}

	public TransactionMessage invalidate(byte[] txHash) {
		// rwLock.writeLock().lock();
		try {// second entry.
			TransactionMessage tm = storage.get(txHash);
			if (tm != null) {
				tm.setRemoved(true);
			}
			rmStorage.put(txHash, System.currentTimeMillis());
			return tm;
		} catch (Exception e) {
			log.error("", e);
			return null;
		} finally {
		}
	}

	public TransactionMessage revalidate(byte[] txHash) {
		try {// second entry.
			TransactionMessage tm = storage.get(txHash);
			if (tm != null && tm.isRemoved()) {
				tm.setRemoved(false);
				rmStorage.remove(txHash);
			}
			return tm;
		} catch (Exception e) {
			log.error("", e);
			return null;
		} finally {
		}
	}

	public synchronized void clear() {
		if (System.currentTimeMillis() - lastClearTime < clearWaitMS && tmConfirmQueue.size() < maxElementsInMemory * 2
				&& storage.size() < maxElementsInMemory * 2) {
			return;
		}
		gcShouldStopChecker.set(false);
		long ccs[] = new long[4];
		long cost[] = new long[4];
		try {
			long start = System.currentTimeMillis();
			ccs[0] = clearQueue();
			cost[0] = (System.currentTimeMillis() - start);
		} catch (Exception e1) {
			log.error("error in clearQueue:", e1);
		}
		try {
			long start = System.currentTimeMillis();
			ccs[1] = clearStorage();
			cost[1] = (System.currentTimeMillis() - start);
		} catch (Exception e) {
			log.error("error in clearStorage:", e);
		}

		try {
			long start = System.currentTimeMillis();
			ccs[2] = clearRemoveQueue();
			cost[2] = (System.currentTimeMillis() - start);
		} catch (Exception e) {
			log.error("error in clearRemoveQueue:", e);
		}
	}

	private void put(byte[] key, TransactionMessage tm) {
		if (storage.size() < this.maxElementsInMemory || tm.getTx() != null) {
			storage.put(key, tm);
			if (storage.size() > this.maxElementsInMemory) {
				clearStorage();
			}
		}
	}

	private int clearQueue() {
		int i = 0;
		int maxtried = tmConfirmQueue.size();
		int clearcount = 0;
		while (i < maxtried && !gcShouldStopChecker.get()) {
			try {
				TransactionMessage tm = tmConfirmQueue.pollFirst();
				if (!tm.isRemoved() && tm.getTx() != null && !rmStorage.containsKey(tm.getKey())) {// 180
					tmConfirmQueue.addLast(tm);
				} else {
					clearcount++;
					storage.remove(tm.getKey());
				}
			} catch (Exception e) {
				log.error("", e);
			} finally {
				i++;
			}
		}
		return clearcount;
	}

	private int clearRemoveQueue() {
		Enumeration<byte[]> en = new Vector<byte[]>(rmStorage.keySet()).elements();
		List<byte[]> removeKeys = new ArrayList<>();
		while (en.hasMoreElements() && !gcShouldStopChecker.get()) {
			try {
				byte[] key = en.nextElement();
				Long rmTime = rmStorage.get(key);
				if (rmTime != null) {
					if (System.currentTimeMillis() - rmTime > 120 * 1000) {
						removeKeys.add(key);
					}
				}
			} catch (Exception e) {
				log.error("", e);
			} finally {
			}
		}
		for (byte[] key : removeKeys) {
			rmStorage.remove(key);
		}

		return removeKeys.size();
	}

	private synchronized int clearStorage() {
		if (System.currentTimeMillis() - lastClearStorage < 5000) {
			return 0;
		}
		if (storage != null) {
			List<byte[]> removeKeys = new ArrayList<>();
			Enumeration<byte[]> en = new Vector<byte[]>(storage.keySet()).elements();
			while (en.hasMoreElements() && !gcShouldStopChecker.get()) {
				try {
					byte[] key = en.nextElement();
					TransactionMessage tm = this.storage.get(key);
					if (tm != null) {
						if (tm.isRemoved()) {
							removeKeys.add(key);
						} else if (tm.getTx() == null
								|| System.currentTimeMillis() - tm.getLastUpdateTime() >= 180 * 1000) {
							removeKeys.add(key);
						} else if (tm.getTx() != null
								&& System.currentTimeMillis() - tm.getLastUpdateTime() >= 60 * 1000) {
						}
					}
				} catch (Exception e) {
					log.error("", e);
				} finally {
				}
			}
			for (byte[] key : removeKeys) {
				storage.remove(key);
			}

			return removeKeys.size();
		} else {
			return 0;
		}
	}
}
