package org.fok.core.trie;

import static org.apache.commons.lang3.concurrent.ConcurrentUtils.constantFuture;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Provides;
import org.bouncycastle.util.encoders.Hex;
import org.fok.core.config.FokChainConfigKeys;
import org.fok.core.cryptoapi.ICryptoHandler;
import org.fok.core.datasource.FokAccountDataAccess;
import org.fok.core.exception.FokTrieRuntimeException;
import org.fok.tools.rlp.FokRLP;
import org.fok.tools.bytes.BytesComparisons;
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
import onight.tfw.outils.conf.PropHelper;
import onight.tfw.outils.pool.ReusefulLoopPool;

import static org.fok.tools.bytes.BytesHelper.*;
import static org.fok.tools.rlp.FokRLP.*;

@NActorProvider
@Provides(specifications = { ActorService.class }, strategy = "SINGLETON")
@Instantiate(name = "fok_state_trie")
@Slf4j
@Data
public class FokStateTrie implements ActorService {
	@ActorRequire(name = "bc_crypto", scope = "global")
	ICryptoHandler crypto;
	@ActorRequire(name = "account_da", scope = "global")
	FokAccountDataAccess accountDA;
	PropHelper prop = new PropHelper(null);

	protected Cache cacheByHash;
	protected static CacheManager cacheManager = CacheManager.create("./conf/ehcache.xml");

	public FokStateTrie() {
		this.cacheByHash = new Cache(
				prop.get(FokChainConfigKeys.account_state_trie_cache_nameId_key, "state_trie_cache"),
				prop.get(FokChainConfigKeys.account_state_trie_cache_size_key, 500), MemoryStoreEvictionPolicy.LRU,
				true, "./" + prop.get(FokChainConfigKeys.account_state_trie_cache_nameId_key, "transaction_cache"),
				true, 0, 0, true, 120, null);
		cacheManager.addCache(this.cacheByHash);
	}

	private final static Object NULL_NODE = new Object();
	private final static int MIN_BRANCHES_CONCURRENTLY = 4;// Math.min(16,Runtime.getRuntime().availableProcessors());
	private static ExecutorService executor = new ForkJoinPool(new PropHelper(null)
			.get("org.brewchain.account.state.parallel", Runtime.getRuntime().availableProcessors() * 2));

	public static ExecutorService getExecutor() {
		return executor;
	}

	public enum NodeType {
		BranchNode, KVNodeValue, KVNodeNode
	}

	public class BatchStorage {
		public BytesHashMap<byte[]> kvs = new BytesHashMap<>();

		public void add(byte[] key, byte[] v) {
			kvs.put(key, v);
		}

		public void remove(byte[] key) {
			kvs.remove(key);
		}
	}

	ThreadLocal<BatchStorage> batchStorage = new ThreadLocal<>();
	ReusefulLoopPool<BatchStorage> bsPool = new ReusefulLoopPool<>();

	public final class Node {
		private byte[] hash = null;
		private byte[] rlp = null;
		private FokRLP.LList parsedRlp = null;
		private boolean dirty = false;

		private Object[] children = null;

		// new empty BranchNode
		public Node() {
			children = new Object[17];
			dirty = true;
		}

		// new KVNode with key and (value or node)
		public Node(FokTrieKey key, Object valueOrNode) {
			this(new Object[] { key, valueOrNode });
			dirty = true;
		}

		// new Node with hash or RLP
		public Node(byte[] hashOrRlp) {
			if (hashOrRlp.length == 32) {
				this.hash = hashOrRlp;
			} else {
				this.rlp = hashOrRlp;
			}
		}

		private Node(FokRLP.LList parsedRlp) {
			this.parsedRlp = parsedRlp;
			this.rlp = parsedRlp.getEncoded();
		}

		private Node(Object[] children) {
			this.children = children;
		}

		public boolean resolveCheck() {
			if (rlp != null || parsedRlp != null || hash == null)
				return true;
			rlp = getHash(hash);
			return rlp != null;
		}

		private void resolve() {
			if (!resolveCheck()) {
				throw new FokTrieRuntimeException("Invalid Trie state, can't resolve hash " + Hex.toHexString(hash));
			}
		}

		public void flushBS(BatchStorage bs) {
			int size = bs.kvs.size();
			if (size > 0) {
				try {
					ArrayList<byte[]> oks = new ArrayList<>();
					ArrayList<byte[]> ovs = new ArrayList<>();
					for (Map.Entry<byte[], byte[]> kvs : bs.kvs.entrySet()) {
						oks.add(kvs.getKey());
						ovs.add(kvs.getValue());
					}

					accountDA.batchPutTrie(oks, ovs);
					bs.kvs.clear();
				} catch (Exception e) {
					log.warn("error in flushBS" + e.getMessage(), e);
				}
				// bs.values.clear();
			}
		}

		public byte[] encode() {
			BatchStorage bs = bsPool.borrow();
			if (bs == null) {
				bs = new BatchStorage();
			}
			try {
				batchStorage.set(bs);
				byte[] ret = encode(1, true);
				flushBS(bs);
				return ret;
			} catch (Exception e) {
				log.warn("error encode:" + e.getMessage(), e);
				throw e;
			} finally {
				if (bs != null) {
					if (bsPool.size() < 100) {
						bs.kvs.clear();
						// bs.values.clear();
						bsPool.retobj(bs);
					}
				}
				batchStorage.remove();
			}
		}

		private byte[] encode(final int depth, boolean forceHash) {
			if (!dirty) {
				return hash != null ? encodeElement(hash) : rlp;
			} else {
				NodeType type = getType();
				byte[] ret;
				if (type == NodeType.BranchNode) {
					if (depth == 1 && async) {
						// parallelize encode() on the first trie level only and
						// if there are at least
						// MIN_BRANCHES_CONCURRENTLY branches are modified
						final Object[] encoded = new Object[17];
						int encodeCnt = 0;
						for (int i = 0; i < 16; i++) {
							final Node child = branchNodeGetChild(i);
							if (child == null) {
								encoded[i] = EMPTY_ELEMENT_RLP;
							} else if (!child.dirty) {
								encoded[i] = child.encode(depth + 1, false);
							} else {
								encodeCnt++;
							}
						}
						for (int i = 0; i < 16; i++) {
							if (encoded[i] == null) {
								final Node child = branchNodeGetChild(i);
								if (encodeCnt >= MIN_BRANCHES_CONCURRENTLY) {
									Callable<byte[]> oCallable = new Callable<byte[]>() {
										@Override
										public byte[] call() throws Exception {
											BatchStorage bs = bsPool.borrow();
											if (bs == null) {
												bs = new BatchStorage();
											}
											try {
												if (bs != null) {
													batchStorage.set(bs);
												}
												byte[] ret = child.encode(depth + 1, false);
												// flush
												flushBS(bs);
												return ret;
											} catch (Exception e) {
												log.error("error in exec bs:" + e.getMessage(), e);
												throw e;
											} finally {
												if (bs != null) {
													batchStorage.remove();
													if (bsPool.size() < 100) {
														bs.kvs.clear();
														// bs.values.clear();
														bsPool.retobj(bs);
													}
												}
											}
										}
									};
									encoded[i] = getExecutor().submit(oCallable);
									oCallable = null;
								} else {
									encoded[i] = child.encode(depth + 1, false);
								}
							}
						}
						byte[] value = branchNodeGetValue();
						encoded[16] = constantFuture(encodeElement(value));
						try {
							ret = encodeRlpListFutures(encoded);
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					} else {
						byte[][] encoded = new byte[17][];
						for (int i = 0; i < 16; i++) {
							Node child = branchNodeGetChild(i);
							encoded[i] = child == null ? EMPTY_ELEMENT_RLP : child.encode(depth + 1, false);
						}
						byte[] value = branchNodeGetValue();
						encoded[16] = encodeElement(value);
						ret = encodeList(encoded);
					}
				} else if (type == NodeType.KVNodeNode) {
					ret = encodeList(encodeElement(kvNodeGetKey().toPacked()),
							kvNodeGetChildNode().encode(depth + 1, false));
				} else {
					byte[] value = kvNodeGetValue();
					ret = encodeList(encodeElement(kvNodeGetKey().toPacked()),
							encodeElement(value == null ? EMPTY_BYTE_ARRAY : value));
				}
				if (hash != null) {
					deleteHash(hash);
				}
				dirty = false;

				if (ret.length < 32 && !forceHash) {
					rlp = ret;
					return ret;
				} else {
					hash = crypto.sha3(ret);
					// hash = ret;
					addHash(hash, ret);
					return encodeElement(hash);
				}
			}
		}

		@SafeVarargs
		private final byte[] encodeRlpListFutures(Object... list) throws ExecutionException, InterruptedException {
			byte[][] vals = new byte[list.length][];
			for (int i = 0; i < list.length; i++) {
				if (list[i] instanceof Future) {
					vals[i] = ((Future<byte[]>) list[i]).get();
				} else {
					vals[i] = (byte[]) list[i];
				}
			}
			return encodeList(vals);
		}

		private void parse() {
			if (children != null)
				return;
			resolve();

			FokRLP.LList list = parsedRlp == null ? FokRLP.decodeLazyList(rlp) : parsedRlp;

			if (list.size() == 2) {
				children = new Object[2];
				FokTrieKey key = FokTrieKey.fromPacked(list.getBytes(0));
				children[0] = key;
				if (key.isTerminal()) {
					children[1] = list.getBytes(1);
				} else {
					children[1] = list.isList(1) ? new Node(list.getList(1)) : new Node(list.getBytes(1));
				}
			} else {
				children = new Object[17];
				parsedRlp = list;
			}
		}

		public Node branchNodeGetChild(int hex) {
			parse();
			assert getType() == NodeType.BranchNode;
			Object n = children[hex];
			if (n == null && parsedRlp != null) {
				if (parsedRlp.isList(hex)) {
					n = new Node(parsedRlp.getList(hex));
				} else {
					byte[] bytes = parsedRlp.getBytes(hex);
					if (bytes.length == 0) {
						n = NULL_NODE;
					} else {
						n = new Node(bytes);
					}
				}
				children[hex] = n;
			}
			return n == NULL_NODE ? null : (Node) n;
		}

		public Node branchNodeSetChild(int hex, Node node) {
			parse();
			assert getType() == NodeType.BranchNode;
			children[hex] = node == null ? NULL_NODE : node;
			dirty = true;
			return this;
		}

		public byte[] branchNodeGetValue() {
			parse();
			assert getType() == NodeType.BranchNode;
			Object n = children[16];
			if (n == null && parsedRlp != null) {
				byte[] bytes = parsedRlp.getBytes(16);
				if (bytes.length == 0) {
					n = NULL_NODE;
				} else {
					n = bytes;
				}
				children[16] = n;
			}
			return n == NULL_NODE ? null : (byte[]) n;
		}

		public Node branchNodeSetValue(byte[] val) {
			parse();
			assert getType() == NodeType.BranchNode;
			children[16] = val == null ? NULL_NODE : val;
			dirty = true;
			return this;
		}

		public int branchNodeCompactIdx() {
			parse();
			assert getType() == NodeType.BranchNode;
			int cnt = 0;
			int idx = -1;
			for (int i = 0; i < 16; i++) {
				if (branchNodeGetChild(i) != null) {
					cnt++;
					idx = i;
					if (cnt > 1)
						return -1;
				}
			}
			return cnt > 0 ? idx : (branchNodeGetValue() == null ? -1 : 16);
		}

		public boolean branchNodeCanCompact() {
			parse();
			assert getType() == NodeType.BranchNode;
			int cnt = 0;
			for (int i = 0; i < 16; i++) {
				cnt += branchNodeGetChild(i) == null ? 0 : 1;
				if (cnt > 1)
					return false;
			}
			return cnt == 0 || branchNodeGetValue() == null;
		}

		public FokTrieKey kvNodeGetKey() {
			parse();
			assert getType() != NodeType.BranchNode;
			return (FokTrieKey) children[0];
		}

		public Node kvNodeGetChildNode() {
			parse();
			assert getType() == NodeType.KVNodeNode;
			return (Node) children[1];
		}

		public byte[] kvNodeGetValue() {
			parse();
			assert getType() == NodeType.KVNodeValue;
			return (byte[]) children[1];
		}

		public Node kvNodeSetValue(byte[] value) {
			parse();
			assert getType() == NodeType.KVNodeValue;
			children[1] = value;
			dirty = true;
			return this;
		}

		public Object kvNodeGetValueOrNode() {
			parse();
			assert getType() != NodeType.BranchNode;
			return children[1];
		}

		public Node kvNodeSetValueOrNode(Object valueOrNode) {
			parse();
			assert getType() != NodeType.BranchNode;
			children[1] = valueOrNode;
			dirty = true;
			return this;
		}

		public NodeType getType() {
			parse();

			return children.length == 17 ? NodeType.BranchNode
					: (children[1] instanceof Node ? NodeType.KVNodeNode : NodeType.KVNodeValue);
		}

		public void dispose() {
			if (hash != null) {
				deleteHash(hash);
			}
		}

		public Node invalidate() {
			dirty = true;
			return this;
		}

		/*********** Dump methods ************/

		// public String dumpStruct(String indent, String prefix) {
		// String ret = indent + prefix + getType() + (dirty ? " *" : "")
		// + (hash == null ? "" : "(hash: " + Hex.toHexString(hash).substring(0,
		// 6) + ")");
		// if (getType() == NodeType.BranchNode) {
		// byte[] value = branchNodeGetValue();
		// ret += (value == null ? "" : " [T] = " + Hex.toHexString(value)) +
		// "\n";
		// for (int i = 0; i < 16; i++) {
		// Node child = branchNodeGetChild(i);
		// if (child != null) {
		// ret += child.dumpStruct(indent + " ", "[" + i + "] ");
		// }
		// }
		//
		// } else if (getType() == NodeType.KVNodeNode) {
		// ret += " [" + kvNodeGetKey() + "]\n";
		// ret += kvNodeGetChildNode().dumpStruct(indent + " ", "");
		// } else {
		// ret += " [" + kvNodeGetKey() + "] = " +
		// Hex.toHexString(kvNodeGetValue()) + "\n";
		// }
		// return ret;
		// }

		// public List<String> dumpTrieNode(boolean compact) {
		// List<String> ret = new ArrayList<>();
		// if (hash != null) {
		// ret.add(hash2str(hash, compact) + " ==> " + dumpContent(false,
		// compact));
		// }
		//
		// if (getType() == NodeType.BranchNode) {
		// for (int i = 0; i < 16; i++) {
		// Node child = branchNodeGetChild(i);
		// if (child != null)
		// ret.addAll(child.dumpTrieNode(compact));
		// }
		// } else if (getType() == NodeType.KVNodeNode) {
		// ret.addAll(kvNodeGetChildNode().dumpTrieNode(compact));
		// }
		// return ret;
		// }
		//
		// private String dumpContent(boolean recursion, boolean compact) {
		// if (recursion && hash != null)
		// return hash2str(hash, compact);
		// String ret;
		// if (getType() == NodeType.BranchNode) {
		// ret = "[";
		// for (int i = 0; i < 16; i++) {
		// Node child = branchNodeGetChild(i);
		// ret += i == 0 ? "" : ",";
		// ret += child == null ? "" : child.dumpContent(true, compact);
		// }
		// byte[] value = branchNodeGetValue();
		// ret += value == null ? "" : ", " + val2str(value, compact);
		// ret += "]";
		// } else if (getType() == NodeType.KVNodeNode) {
		// ret = "[<" + kvNodeGetKey() + ">, " +
		// kvNodeGetChildNode().dumpContent(true, compact) + "]";
		// } else {
		// ret = "[<" + kvNodeGetKey() + ">, " + val2str(kvNodeGetValue(),
		// compact) + "]";
		// }
		// return ret;
		// }

		@Override
		public String toString() {
			return getType() + (dirty ? " *" : "") + (hash == null ? "" : "(hash: " + Hex.toHexString(hash) + " )");
		}
	}

	public interface ScanAction {

		void doOnNode(byte[] hash, Node node);

		void doOnValue(byte[] nodeHash, Node node, byte[] key, byte[] value);
	}

	// private TrieCache cache;
	private Node root;
	private boolean async = true;

	// public StateTrie() {
	// this((byte[]) null);
	// }
	//
	// public StateTrie(byte[] root) {
	// setRoot(root);
	// }

	public void setAsync(boolean async) {
		this.async = async;
	}

	private void encode() {
		if (root != null) {
			root.encode();
		}
	}

	public void setRoot(byte[] root) {
		if (root != null && !BytesComparisons.equal(root, BytesHelper.EMPTY_BYTE_ARRAY)) {
			this.root = new Node(root);
		} else {
			this.root = null;
		}

	}

	private boolean hasRoot() {
		return root != null && root.resolveCheck();
	}
	// Cache<String, byte[]> cacheByHash =
	// CacheBuilder.newBuilder().initialCapacity(10000)
	// .expireAfterWrite(300, TimeUnit.SECONDS).maximumSize(200000)
	// .concurrencyLevel(Runtime.getRuntime().availableProcessors()).build();

	private byte[] getHash(byte[] hash) {
		String hexHash = crypto.bytesToHexStr(hash);
		byte[] v = null;
		try {
			Element element = cacheByHash.get(hexHash);
			if (element != null) {
				return (byte[]) element.getObjectValue();
			}
			v = accountDA.getTrie(hash);

			if (v != null) {
				element = new Element(hash, v);
				cacheByHash.put(element);
				return v;
			}
		} catch (Exception e) {
			log.warn("getHash Error:" + e.getMessage() + ",key=" + hexHash, e);
		}
		return null;
	}

	private void addHash(byte[] hash, byte[] ret) {
		// System.out.println("addHash:" + type + ",hash=" +
		// Hex.toHexString(hash));

		BatchStorage bs = batchStorage.get();
		if (bs != null) {
			bs.add(hash, ret);
			Element element = new Element(hash, ret);
			cacheByHash.put(element);
		} else {
			try {
				accountDA.putTrie(hash, ret);
			} catch (Exception e) {
				log.error("error on add trie, key::" + Hex.toHexString(hash) + " value::" + Hex.toHexString(ret));
			}
		}
	}

	private void deleteHash(byte[] hash) {
		BatchStorage bs = batchStorage.get();
		if (bs != null) {
			bs.remove(hash);
		}
	}

	public byte[] get(byte[] key) {
		if (!hasRoot())
			return null; // treating unknown root hash as empty trie
		FokTrieKey k = FokTrieKey.fromNormal(key);
		return get(root, k);
	}

	private byte[] get(Node n, FokTrieKey k) {
		if (n == null)
			return null;

		NodeType type = n.getType();
		if (type == NodeType.BranchNode) {
			if (k.isEmpty())
				return n.branchNodeGetValue();
			Node childNode = n.branchNodeGetChild(k.getHex(0));
			// childNode.hash
			return get(childNode, k.shift(1));
		} else {
			FokTrieKey k1 = k.matchAndShift(n.kvNodeGetKey());
			if (k1 == null)
				return null;
			if (type == NodeType.KVNodeValue) {
				return k1.isEmpty() ? n.kvNodeGetValue() : null;
			} else {
				return get(n.kvNodeGetChildNode(), k1);
			}
		}
	}

	public void put(byte[] key, byte[] value) {
		FokTrieKey k = FokTrieKey.fromNormal(key);
		if (root == null) {
			if (value != null && value.length > 0) {
				root = new Node(k, value);
			}
		} else {
			if (value == null || value.length == 0) {
				root = delete(root, k);
			} else {
				root = insert(root, k, value);
			}
		}
	}

	private Node insert(Node n, FokTrieKey k, Object nodeOrValue) {
		NodeType type = n.getType();
		if (type == NodeType.BranchNode) {
			if (k.isEmpty())
				return n.branchNodeSetValue((byte[]) nodeOrValue);
			Node childNode = n.branchNodeGetChild(k.getHex(0));
			if (childNode != null) {
				return n.branchNodeSetChild(k.getHex(0), insert(childNode, k.shift(1), nodeOrValue));
			} else {
				FokTrieKey childKey = k.shift(1);
				Node newChildNode;
				if (!childKey.isEmpty()) {
					newChildNode = new Node(childKey, nodeOrValue);
				} else {
					newChildNode = nodeOrValue instanceof Node ? (Node) nodeOrValue : new Node(childKey, nodeOrValue);
				}
				return n.branchNodeSetChild(k.getHex(0), newChildNode);
			}
		} else {
			FokTrieKey commonPrefix = k.getCommonPrefix(n.kvNodeGetKey());
			if (commonPrefix.isEmpty()) {
				Node newBranchNode = new Node();
				insert(newBranchNode, n.kvNodeGetKey(), n.kvNodeGetValueOrNode());
				insert(newBranchNode, k, nodeOrValue);
				n.dispose();
				return newBranchNode;
			} else if (commonPrefix.equals(k)) {
				return n.kvNodeSetValueOrNode(nodeOrValue);
			} else if (commonPrefix.equals(n.kvNodeGetKey())) {
				insert(n.kvNodeGetChildNode(), k.shift(commonPrefix.getLength()), nodeOrValue);
				return n.invalidate();
			} else {
				Node newBranchNode = new Node();
				Node newKvNode = new Node(commonPrefix, newBranchNode);
				// TODO can be optimized
				insert(newKvNode, n.kvNodeGetKey(), n.kvNodeGetValueOrNode());
				insert(newKvNode, k, nodeOrValue);
				n.dispose();
				return newKvNode;
			}
		}
	}

	public void delete(byte[] key) {
		FokTrieKey k = FokTrieKey.fromNormal(key);
		if (root != null) {
			root = delete(root, k);
		}
		cacheByHash.remove(key);
	}

	private Node delete(Node n, FokTrieKey k) {
		NodeType type = n.getType();
		Node newKvNode;
		if (type == NodeType.BranchNode) {
			if (k.isEmpty()) {
				n.branchNodeSetValue(null);
			} else {
				int idx = k.getHex(0);
				Node child = n.branchNodeGetChild(idx);
				if (child == null)
					return n; // no key found

				Node newNode = delete(child, k.shift(1));
				n.branchNodeSetChild(idx, newNode);
				if (newNode != null)
					return n; // newNode != null thus number of children didn't
								// decrease
			}

			// child node or value was deleted and the branch node may need to
			// be compacted
			int compactIdx = n.branchNodeCompactIdx();
			if (compactIdx < 0)
				return n; // no compaction is required

			// only value or a single child left - compact branch node to kvNode
			n.dispose();
			if (compactIdx == 16) { // only value left
				return new Node(FokTrieKey.empty(true), n.branchNodeGetValue());
			} else { // only single child left
				newKvNode = new Node(FokTrieKey.singleHex(compactIdx), n.branchNodeGetChild(compactIdx));
			}
		} else { // n - kvNode
			FokTrieKey k1 = k.matchAndShift(n.kvNodeGetKey());
			if (k1 == null) {
				// no key found
				return n;
			} else if (type == NodeType.KVNodeValue) {
				if (k1.isEmpty()) {
					// delete this kvNode
					n.dispose();
					return null;
				} else {
					// else no key found
					return n;
				}
			} else {
				Node newChild = delete(n.kvNodeGetChildNode(), k1);
				if (newChild == null)
					throw new RuntimeException("Shouldn't happen");
				newKvNode = n.kvNodeSetValueOrNode(newChild);
			}
		}

		// if we get here a new kvNode was created, now need to check
		// if it should be compacted with child kvNode
		Node newChild = newKvNode.kvNodeGetChildNode();
		if (newChild.getType() != NodeType.BranchNode) {
			// two kvNodes should be compacted into a single one
			FokTrieKey newKey = newKvNode.kvNodeGetKey().concat(newChild.kvNodeGetKey());
			Node newNode = new Node(newKey, newChild.kvNodeGetValueOrNode());
			newChild.dispose();
			newKvNode.dispose();
			return newNode;
		} else {
			// no compaction needed
			return newKvNode;
		}
	}

	public byte[] getRootHash() {
		encode();
		return root != null ? root.hash : BytesHelper.EMPTY_BYTE_ARRAY;
	}

	public void clear() {
		if (root != null && root.dirty) {
			// persist all dirty nodes to underlying Source
			// encode();
			// release all Trie Node instances for GC
			root = null;
			root = new Node(root.hash);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		FokStateTrie oStateTrie = (FokStateTrie) o;

		return BytesComparisons.equal(getRootHash(), oStateTrie.getRootHash());

	}

	public void scanTree(ScanAction scanAction) {
		scanTree(root, FokTrieKey.empty(false), scanAction);
	}

	public void scanTree(Node node, FokTrieKey k, ScanAction scanAction) {
		if (node == null)
			return;
		if (node.hash != null) {
			scanAction.doOnNode(node.hash, node);
		}
		if (node.getType() == NodeType.BranchNode) {
			if (node.branchNodeGetValue() != null)
				scanAction.doOnValue(node.hash, node, k.toNormal(), node.branchNodeGetValue());
			for (int i = 0; i < 16; i++) {
				scanTree(node.branchNodeGetChild(i), k.concat(FokTrieKey.singleHex(i)), scanAction);
			}
		} else if (node.getType() == NodeType.KVNodeNode) {
			scanTree(node.kvNodeGetChildNode(), k.concat(node.kvNodeGetKey()), scanAction);
		} else {
			scanAction.doOnValue(node.hash, node, k.concat(node.kvNodeGetKey()).toNormal(), node.kvNodeGetValue());
		}
	}
}
